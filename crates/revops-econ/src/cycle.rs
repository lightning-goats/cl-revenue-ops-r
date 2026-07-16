//! Deterministic cycle core (port of `modules/econ_cycle.py`).
//!
//! The spec's cycle: collect -> close window -> snapshot -> generate
//! intents -> arbitrate -> authorize -> execute -> record -> publish. This
//! module delivers the DETERMINISTIC CORE of that cycle running in shadow:
//!
//! - pure intent generation (v0: REBALANCE intents from the pure planner's
//!   `RebalancePair` candidates),
//! - BATCH arbitration via `crate::arbiter::arbitrate` (the pure J3-ordered
//!   ranking, activated over a real cycle's intent set),
//! - a ledgered, publishable result. NO execution: the shadow cycle holds
//!   no authority.
//!
//! Determinism contract (spec acceptance): identical inputs + identical
//! `CycleContext` produce byte-identical wire output, regardless of input
//! ordering. Pinned by `tests/cycle.rs`, transcribed from
//! `tests/test_econ_cycle.py`.
//!
//! `run_shadow_cycle`'s live collector (reading `rebalance_engine`,
//! ledgering proposed intents, wiring into `EconShadow`) is Phase 2b
//! wiring, deferred per the Task 8 brief — not ported here.
//!
//! ## The float-in-explanation hazard
//!
//! Python's `_rebalance_intent` puts `round(float(score), 6)` into the
//! `cycle_rebalance` explanation's `"score"` component — the ONE place a
//! float legitimately reaches `CycleResult.to_wire()`. Rust's
//! `revops_core::canonical::canonical_json` rejects every float-typed
//! number fail-closed BY DESIGN (see that function's doc comment) because
//! it also serializes idempotency-key inputs, where a divergent float
//! formatter would silently produce a different key with nothing failing.
//! `CycleResult::canonical()` below does NOT use `revops_core`'s
//! canonical writer for that reason. It uses a local writer
//! (`canonical_json_with_pyfloat`) that mirrors the exact same semantics
//! (sorted object keys, `json.dumps(sort_keys=True, separators=(",",":"),
//! ensure_ascii=False)`-compatible escaping) but renders float-typed
//! leaves through `crate::pyfloat::py_repr` instead of rejecting them.
//!
//! This local writer is used ONLY by `CycleResult::canonical()`, for
//! shadow-cycle wire publishing. It is NEVER used for idempotency keys —
//! those are computed inside `crate::intents::compute_idempotency_key`
//! exclusively via `revops_core::canonical_json`, which never sees a float
//! (the five-field idempotency subset is integer/string/null only). The
//! conformance corpus contains no float wire values, so the Phase 2 gate
//! does not exercise `py_repr` at all; the live diff harness verifies it
//! against real Python output during shadow (Phase 2b).

use serde_json::{json, Value};

use crate::arbiter::{arbitrate, ArbitrationResult};
use crate::context::CycleContext;
use crate::ev::{benefit_msat_from_sats, confidence_micro};
use crate::intents::{make_intent, to_wire, Explanation, IntentEnvelope, IntentFields};
use crate::pyfloat::py_repr;
use crate::pyfloat::py_round;
use crate::types::{EconResult, Micro, Msat, SignedMsat};

pub const SCHEMA_NAME: &str = "econ_cycle_result";
pub const SCHEMA_VERSION: i64 = 0;

/// A pure-planner rebalance candidate (duck-typed `pair` in the Python
/// source, made explicit). `score_decomposition`, when present, is expected
/// to be a JSON object carrying at least `final_score_sats`/`p_success`
/// keys (see `rebalance_intent` below) — any other shape is simply treated
/// as "no EV data" rather than an error, mirroring Python's
/// `decomp.get(...)` duck-typing.
#[derive(Debug, Clone)]
pub struct RebalancePair {
    pub source_channel_id: String,
    pub dest_channel_id: String,
    pub amount_sats: i64,
    pub pair_budget_sats: i64,
    pub score: f64,
    pub score_decomposition: Option<serde_json::Value>,
}

/// The pure cycle core's result: a batch-arbitrated set of REBALANCE
/// intents plus the context/count that produced them. Never executes
/// anything — no I/O, no clock, no authority.
#[derive(Debug, Clone)]
pub struct CycleResult {
    pub context: CycleContext,
    pub channel_count: i64,
    pub intents: Vec<IntentEnvelope>,
    pub arbitration: ArbitrationResult,
}

impl CycleResult {
    /// The `econ_cycle_result` v0 wire shape, exact key set and names as
    /// Python's `CycleResult.to_wire()`.
    pub fn to_wire(&self) -> Value {
        let ordered: Vec<Value> = self.arbitration.ordered.iter().map(to_wire).collect();
        let rejected: Vec<Value> = self
            .arbitration
            .rejected
            .iter()
            .map(|(env, code, detail)| {
                json!({
                    "intent": to_wire(env),
                    "reason_code": code,
                    "detail": detail,
                })
            })
            .collect();
        let superseded: serde_json::Map<String, Value> = self
            .arbitration
            .superseded
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect();
        json!({
            "schema_name": SCHEMA_NAME,
            "schema_version": SCHEMA_VERSION,
            "cycle_id": self.context.cycle_id,
            "cycle_time": self.context.cycle_time.value(),
            "seed": self.context.seed,
            "snapshot_id": self.context.snapshot_id,
            "channel_count": self.channel_count,
            "intents_proposed": self.intents.len() as i64,
            "ordered": ordered,
            "rejected": rejected,
            "superseded": Value::Object(superseded),
        })
    }

    /// Canonical wire serialization for shadow publishing — see the
    /// module doc comment's "float-in-explanation hazard" section for why
    /// this is NOT `revops_core::canonical_json` and must never back an
    /// idempotency key.
    pub fn canonical(&self) -> EconResult<String> {
        let mut out = String::new();
        write_value_with_pyfloat(&self.to_wire(), &mut out);
        Ok(out)
    }
}

/// Like [`rebalance_intent_pairs`] but drops the pair association —
/// callers that only need the envelopes (no execution-cutover mapping).
pub fn rebalance_intents_from_pairs(
    pairs: &[RebalancePair],
    ctx: &CycleContext,
) -> EconResult<Vec<IntentEnvelope>> {
    Ok(rebalance_intent_pairs(pairs, ctx, false)?
        .into_iter()
        .map(|(env, _idx)| env)
        .collect())
}

/// Maps pure-planner `RebalancePair` candidates to REBALANCE intent
/// envelopes, keeping the `(envelope, pair index)` association (the
/// execution cutover needs to map arbitrated envelopes back to their
/// candidates). Deterministic: envelope fields derive only from pair data
/// + `ctx`.
///
/// Pairs are processed in `(dest_channel_id, source_channel_id)` sorted
/// order — matching Python's `sorted(pairs, key=lambda p: (str(p.dest...),
/// str(p.source...)))` — via a stable sort, so ties keep their original
/// relative order exactly as Python's Timsort would.
///
/// `ev_enabled` (PR 6, `econ_ev_populated`): populate the envelope's
/// EV/confidence from `pair.score_decomposition` (`final_score_sats` /
/// `p_success`). Default `false` keeps the byte-pinned zeros.
pub fn rebalance_intent_pairs(
    pairs: &[RebalancePair],
    ctx: &CycleContext,
    ev_enabled: bool,
) -> EconResult<Vec<(IntentEnvelope, usize)>> {
    let mut order: Vec<usize> = (0..pairs.len()).collect();
    order.sort_by(|&a, &b| {
        (
            pairs[a].dest_channel_id.as_str(),
            pairs[a].source_channel_id.as_str(),
        )
            .cmp(&(
                pairs[b].dest_channel_id.as_str(),
                pairs[b].source_channel_id.as_str(),
            ))
    });
    let mut out = Vec::with_capacity(pairs.len());
    for idx in order {
        let env = rebalance_intent(&pairs[idx], ctx, ev_enabled)?;
        out.push((env, idx));
    }
    Ok(out)
}

/// Builds one REBALANCE `IntentEnvelope` from a pair (port of Python's
/// `_rebalance_intent`): 600s expiry, priority 50, bucket `"rebalance"`,
/// origin `"econ_cycle_shadow"`, `reversible=false`; `amount_msat` and
/// `capital_committed_msat` both `amount_sats*1000`; `max_cost_msat`
/// `pair_budget_sats*1000`; explanation kind `"cycle_rebalance"` with
/// components `source`/`dest`/`amount_sats`/`score` (score rounded to 6
/// decimal places, mirroring `round(float(score), 6)`).
fn rebalance_intent(
    pair: &RebalancePair,
    ctx: &CycleContext,
    ev_enabled: bool,
) -> EconResult<IntentEnvelope> {
    let amount = pair.amount_sats;
    let max_fee = pair.pair_budget_sats;

    let mut benefit = SignedMsat(0);
    let mut confidence = Micro::new(0).expect("0 is always a valid Micro");
    if ev_enabled {
        if let Some(decomp) = pair.score_decomposition.as_ref().and_then(Value::as_object) {
            if decomp.contains_key("final_score_sats") {
                let final_score_sats = decomp.get("final_score_sats").and_then(Value::as_f64);
                benefit = benefit_msat_from_sats(final_score_sats)?;
                let p_success = decomp.get("p_success").and_then(Value::as_f64);
                confidence = confidence_micro(p_success);
            }
        }
    }

    let amount_msat = Msat::from_sats(amount)?;
    let max_cost_msat = Msat::from_sats(max_fee)?;
    let capital_committed_msat = amount_msat;

    let rounded_score = py_round(pair.score, 6);
    let score_value = if rounded_score.is_finite() {
        json!(rounded_score)
    } else {
        // Never reachable from a real economic score; fail closed rather
        // than embed a NaN/Infinity JSON has no representation for.
        return Err(crate::types::EconError {
            msg: format!("rebalance_intent: non-finite score: {:?}", pair.score),
        });
    };

    make_intent(IntentFields {
        intent_type: "REBALANCE".to_string(),
        snapshot_id: ctx.snapshot_id.clone(),
        created_at: ctx.cycle_time,
        expires_at: ctx.cycle_time.plus_seconds(600)?,
        target: pair.dest_channel_id.clone(),
        amount_msat: Some(amount_msat),
        expected_benefit_msat: benefit,
        max_cost_msat,
        capital_committed_msat,
        confidence_micro: confidence,
        reason_codes: vec![],
        explanation: Explanation {
            kind: "cycle_rebalance".to_string(),
            components: vec![
                (
                    "source".to_string(),
                    Value::String(pair.source_channel_id.clone()),
                ),
                (
                    "dest".to_string(),
                    Value::String(pair.dest_channel_id.clone()),
                ),
                ("amount_sats".to_string(), json!(amount)),
                ("score".to_string(), score_value),
            ],
        },
        preconditions: vec![],
        priority: 50,
        budget_bucket: "rebalance".to_string(),
        origin_policy: "econ_cycle_shadow".to_string(),
        reversible: false,
    })
}

/// The pure cycle core: intents from policy outputs, then batch
/// arbitration under the J3 ladder. No I/O, no clock, no execution.
/// Mirrors Python's `plan_cycle(*, pairs, ctx, channel_count=0)`
/// (`arbitrate`'s `extended_rules` defaults to `false`, same as Python's
/// call site).
pub fn plan_cycle(
    pairs: &[RebalancePair],
    ctx: &CycleContext,
    channel_count: i64,
) -> EconResult<CycleResult> {
    let intents = rebalance_intents_from_pairs(pairs, ctx)?;
    let arbitration = arbitrate(&intents, ctx.cycle_time.value(), false);
    Ok(CycleResult {
        context: ctx.clone(),
        channel_count,
        intents,
        arbitration,
    })
}

/// Local canonical writer for shadow-cycle publishing ONLY — see the
/// module doc comment. Mirrors `revops_core::canonical::canonical_json`'s
/// semantics (sorted object keys, `json.dumps(sort_keys=True,
/// separators=(",",":"), ensure_ascii=False)`-compatible string escaping)
/// except float-typed numbers are rendered via `pyfloat::py_repr` instead
/// of being rejected.
fn write_value_with_pyfloat(v: &Value, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(n) => {
            if n.is_f64() {
                out.push_str(&py_repr(
                    n.as_f64().expect("is_f64() implies as_f64() succeeds"),
                ));
            } else {
                out.push_str(&n.to_string());
            }
        }
        Value::String(s) => write_string_escaped(s, out),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_value_with_pyfloat(item, out);
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_string_escaped(k, out);
                out.push(':');
                write_value_with_pyfloat(&map[*k], out);
            }
            out.push('}');
        }
    }
}

/// Byte-for-byte identical escaping rules to
/// `revops_core::canonical::canonical_json`'s private `write_string`
/// (duplicated rather than shared: that helper is private to a crate this
/// task must not modify — see the Global Constraints' worktree-isolation
/// rule).
fn write_string_escaped(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}
