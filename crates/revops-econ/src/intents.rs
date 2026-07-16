//! Intent envelopes + idempotency keys (port of `modules/econ_intents.py`).
//!
//! An intent is a PROPOSAL, never an authorization. Every policy decision
//! becomes an `IntentEnvelope` carrying:
//!
//! - a deterministic idempotency key: sha256 of the canonical JSON (J3) of
//!   exactly `(intent_type, target, amount_msat, snapshot_id,
//!   budget_bucket)`
//! - a structured `Explanation` — human-readable text is RENDERED from the
//!   structure, never authored separately
//! - checked domain types for every amount (`crate::types`)
//! - stable reason codes (`crate::reason::CATALOG`, via `ALL`/`is_valid_code`)
//!
//! Wire contract: `schemas/intent.v0.schema.json` (Python side).

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::reason;
use crate::types::{EconError, EconResult, IntentId, Micro, Msat, SignedMsat, UnixTime};

pub const SCHEMA_NAME: &str = "intent";
pub const SCHEMA_VERSION: i64 = 0;

/// Workstream B minimum intent vocabulary — stable wire strings, exact
/// order of the Python `INTENT_TYPES` tuple.
pub const INTENT_TYPES: [&str; 9] = [
    "SET_FEE",
    "SET_HTLC_MAX",
    "REBALANCE",
    "OPEN_CHANNEL",
    "CLOSE_CHANNEL",
    "SWAP_IN",
    "SWAP_OUT",
    "JOIN_LIQUIDITY_SWAP",
    "MAINTAIN_ONCHAIN_RESERVE",
];

/// Structured decision explanation; text derives from structure, never the
/// reverse. Mirrors Python's `Explanation` dataclass; `components` is a
/// `Vec` here rather than a `Tuple` (Rust has no fixed-size heterogeneous
/// tuple-of-pairs equivalent that's this ergonomic).
#[derive(Debug, Clone, PartialEq)]
pub struct Explanation {
    pub kind: String,
    pub components: Vec<(String, Value)>,
}

impl Explanation {
    /// Renders `"{kind}: {name}={value}, {name}={value}, ..."`, matching
    /// Python's `f"{name}={value}"` join semantics for each component
    /// value's `str()`:
    /// - `Value::String` -> raw, unquoted
    /// - integer `Value::Number` -> plain digits
    /// - `Value::Bool` -> `"True"`/`"False"` (Python capitalization, not
    ///   Rust's `true`/`false`)
    /// - `Value::Null` -> `"None"`
    /// - float-typed `Value::Number` -> **not supported** in Phase 2 (see
    ///   `pyfloat.rs`'s doc comment and the Task 8 "float-in-explanation
    ///   hazard" note in the phase plan) — panics rather than silently
    ///   rendering a Rust `f64::to_string()` that would diverge from
    ///   Python's `repr(float)` and break wire byte-parity.
    pub fn render(&self) -> String {
        let parts: Vec<String> = self
            .components
            .iter()
            .map(|(name, value)| format!("{name}={}", render_component_value(value)))
            .collect();
        format!("{}: {}", self.kind, parts.join(", "))
    }
}

fn render_component_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => {
            if n.is_f64() {
                panic!(
                    "Explanation::render: float component values are not supported in \
                     Phase 2 (see pyfloat.rs); got {n}"
                );
            }
            n.to_string()
        }
        Value::Bool(b) => if *b { "True" } else { "False" }.to_string(),
        Value::Null => "None".to_string(),
        other => other.to_string(),
    }
}

/// Deterministic key (J3): canonical JSON of EXACTLY these five fields,
/// sha256 hex. `amount_msat: None` -> JSON `null`. Byte-exact with Python:
/// `sha256(canonical_json({"amount_msat":…,"budget_bucket":…,
/// "intent_type":…,"snapshot_id":…,"target":…})).hexdigest()` — key order
/// in the object is irrelevant, `canonical_json` sorts.
pub fn compute_idempotency_key(
    intent_type: &str,
    target: &str,
    amount_msat: Option<i64>,
    snapshot_id: &str,
    budget_bucket: &str,
) -> String {
    let subset = json!({
        "intent_type": intent_type,
        "target": target,
        "amount_msat": amount_msat,
        "snapshot_id": snapshot_id,
        "budget_bucket": budget_bucket,
    });
    let canon = revops_core::canonical::canonical_json(&subset)
        .expect("five-field subset is integer/string/null only");
    hex::encode(Sha256::digest(canon.as_bytes()))
}

/// A fully-populated economic intent envelope. Every amount is a checked
/// domain type (`crate::types`); `intent_id`/`idempotency_key` are always
/// derived (see `make_intent`), never supplied directly by a caller other
/// than `from_wire` reconstructing a previously-derived envelope.
#[derive(Debug, Clone, PartialEq)]
pub struct IntentEnvelope {
    pub intent_id: IntentId,
    pub intent_type: String,
    pub idempotency_key: String,
    pub snapshot_id: String,
    pub created_at: UnixTime,
    pub expires_at: UnixTime,
    pub target: String,
    pub amount_msat: Option<Msat>,
    pub expected_benefit_msat: SignedMsat,
    pub max_cost_msat: Msat,
    pub capital_committed_msat: Msat,
    pub confidence_micro: Micro,
    pub reason_codes: Vec<String>,
    pub explanation: Explanation,
    pub preconditions: Vec<String>,
    pub priority: i32,
    pub budget_bucket: String,
    pub origin_policy: String,
    pub reversible: bool,
    pub schema_name: String,
    pub schema_version: i64,
}

impl IntentEnvelope {
    /// Mirrors Python's `IntentEnvelope.__post_init__` exactly: intent_type
    /// membership, non-empty snapshot_id/target/budget_bucket/origin_policy,
    /// non-empty explanation.kind, `expires_at > created_at`, `0 <= priority <= 100`,
    /// and every reason code present in the catalog. (Python also type-checks
    /// `amount_msat` is `Msat | None`; here that's enforced by the
    /// `Option<Msat>` field type itself, so there is nothing further to check.)
    fn validate(&self) -> EconResult<()> {
        if !INTENT_TYPES.contains(&self.intent_type.as_str()) {
            return Err(EconError {
                msg: format!("unknown intent_type: {:?}", self.intent_type),
            });
        }
        if self.snapshot_id.is_empty() || self.target.is_empty() {
            return Err(EconError {
                msg: "snapshot_id and target required".to_string(),
            });
        }
        if self.budget_bucket.is_empty() || self.origin_policy.is_empty() {
            return Err(EconError {
                msg: "budget_bucket and origin_policy required".to_string(),
            });
        }
        if self.explanation.kind.is_empty() {
            return Err(EconError {
                msg: "Explanation.kind must be non-empty".to_string(),
            });
        }
        if self.expires_at.value() <= self.created_at.value() {
            return Err(EconError {
                msg: format!(
                    "expires_at ({}) must exceed created_at ({})",
                    self.expires_at.value(),
                    self.created_at.value()
                ),
            });
        }
        if !(0..=100).contains(&self.priority) {
            return Err(EconError {
                msg: format!("priority 0-100: {}", self.priority),
            });
        }
        for code in &self.reason_codes {
            if !reason::is_valid_code(code) {
                return Err(EconError {
                    msg: format!("unknown reason code: {code:?}"),
                });
            }
        }
        Ok(())
    }
}

/// The `make_intent` input: everything an `IntentEnvelope` needs except
/// `intent_id`/`idempotency_key` (always derived) and
/// `schema_name`/`schema_version` (always the module constants).
#[derive(Debug, Clone)]
pub struct IntentFields {
    pub intent_type: String,
    pub snapshot_id: String,
    pub created_at: UnixTime,
    pub expires_at: UnixTime,
    pub target: String,
    pub amount_msat: Option<Msat>,
    pub expected_benefit_msat: SignedMsat,
    pub max_cost_msat: Msat,
    pub capital_committed_msat: Msat,
    pub confidence_micro: Micro,
    pub reason_codes: Vec<String>,
    pub explanation: Explanation,
    pub preconditions: Vec<String>,
    pub priority: i32,
    pub budget_bucket: String,
    pub origin_policy: String,
    pub reversible: bool,
}

/// Construct an envelope, deriving the idempotency key and intent_id
/// (`"int-" + key[:16]`), then validating (mirrors Python's `make_intent`
/// followed by `IntentEnvelope.__post_init__`).
pub fn make_intent(fields: IntentFields) -> EconResult<IntentEnvelope> {
    let key = compute_idempotency_key(
        &fields.intent_type,
        &fields.target,
        fields.amount_msat.map(Msat::value),
        &fields.snapshot_id,
        &fields.budget_bucket,
    );
    let intent_id = IntentId::new(format!("int-{}", &key[..16]))?;
    let env = IntentEnvelope {
        intent_id,
        idempotency_key: key,
        intent_type: fields.intent_type,
        snapshot_id: fields.snapshot_id,
        created_at: fields.created_at,
        expires_at: fields.expires_at,
        target: fields.target,
        amount_msat: fields.amount_msat,
        expected_benefit_msat: fields.expected_benefit_msat,
        max_cost_msat: fields.max_cost_msat,
        capital_committed_msat: fields.capital_committed_msat,
        confidence_micro: fields.confidence_micro,
        reason_codes: fields.reason_codes,
        explanation: fields.explanation,
        preconditions: fields.preconditions,
        priority: fields.priority,
        budget_bucket: fields.budget_bucket,
        origin_policy: fields.origin_policy,
        reversible: fields.reversible,
        schema_name: SCHEMA_NAME.to_string(),
        schema_version: SCHEMA_VERSION,
    };
    env.validate()?;
    Ok(env)
}

/// Inclusive boundary: `now >= expires_at` counts as expired (corpus s34
/// `tests/conformance/scenarios/34-expired-intent/case.json` pins probes
/// `expires_at + {-1, 0, 1}` i.e. `NOW+599/600/601` -> `[false, true,
/// true]` against an intent with `expires_at: 1752400600`).
pub fn is_expired(env: &IntentEnvelope, now: UnixTime) -> bool {
    now.value() >= env.expires_at.value()
}

/// Serialize to the wire dict shape, matching Python's `to_wire` field
/// order and null handling exactly. `explanation.components` renders as
/// `[[name, value], ...]` (arrays, not objects — matches Python's list of
/// 2-tuples via `json.dumps`).
pub fn to_wire(env: &IntentEnvelope) -> Value {
    let components: Vec<Value> = env
        .explanation
        .components
        .iter()
        .map(|(name, value)| json!([name, value]))
        .collect();
    json!({
        "schema_name": env.schema_name,
        "schema_version": env.schema_version,
        "intent_id": env.intent_id.as_str(),
        "intent_type": env.intent_type,
        "idempotency_key": env.idempotency_key,
        "snapshot_id": env.snapshot_id,
        "created_at": env.created_at.value(),
        "expires_at": env.expires_at.value(),
        "target": env.target,
        "amount_msat": env.amount_msat.map(Msat::value),
        "expected_benefit_msat": env.expected_benefit_msat.0,
        "max_cost_msat": env.max_cost_msat.value(),
        "capital_committed_msat": env.capital_committed_msat.value(),
        "confidence_micro": env.confidence_micro.value(),
        "reason_codes": env.reason_codes,
        "explanation": {
            "kind": env.explanation.kind,
            "components": components,
        },
        "preconditions": env.preconditions,
        "priority": env.priority,
        "budget_bucket": env.budget_bucket,
        "origin_policy": env.origin_policy,
        "reversible": env.reversible,
    })
}

fn wire_str(d: &Value, key: &str) -> EconResult<String> {
    d.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| EconError {
            msg: format!("intent wire: missing/invalid string field {key:?}"),
        })
}

fn wire_i64(d: &Value, key: &str) -> EconResult<i64> {
    d.get(key).and_then(Value::as_i64).ok_or_else(|| EconError {
        msg: format!("intent wire: missing/invalid integer field {key:?}"),
    })
}

fn wire_bool(d: &Value, key: &str) -> EconResult<bool> {
    d.get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| EconError {
            msg: format!("intent wire: missing/invalid boolean field {key:?}"),
        })
}

fn wire_str_array(d: &Value, key: &str) -> EconResult<Vec<String>> {
    d.get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| EconError {
            msg: format!("intent wire: missing/invalid array field {key:?}"),
        })?
        .iter()
        .map(|v| {
            v.as_str().map(str::to_string).ok_or_else(|| EconError {
                msg: format!("intent wire: {key} entries must be strings"),
            })
        })
        .collect()
}

/// Deserialize from the wire dict shape. Strict `schema_name`/
/// `schema_version` check up front (mirrors Python's `from_wire` early
/// raise, before it would even attempt to construct the envelope); every
/// other field is parsed through the same checked domain-type constructors
/// used everywhere else, so an out-of-range or malformed value fails
/// closed with `EconError` rather than panicking or silently coercing.
pub fn from_wire(d: &Value) -> EconResult<IntentEnvelope> {
    let schema_name = d.get("schema_name").and_then(Value::as_str);
    let schema_version = d.get("schema_version").and_then(Value::as_i64);
    if schema_name != Some(SCHEMA_NAME) || schema_version != Some(SCHEMA_VERSION) {
        return Err(EconError {
            msg: format!("intent wire schema mismatch: {schema_name:?} v{schema_version:?}"),
        });
    }

    let amount_msat = match d.get("amount_msat") {
        None | Some(Value::Null) => None,
        Some(v) => {
            let raw = v.as_i64().ok_or_else(|| EconError {
                msg: "intent wire: amount_msat must be an integer or null".to_string(),
            })?;
            Some(Msat::new(raw)?)
        }
    };

    let explanation_value = d.get("explanation").ok_or_else(|| EconError {
        msg: "intent wire: missing field \"explanation\"".to_string(),
    })?;
    let kind = wire_str(explanation_value, "kind")?;
    let components_raw = explanation_value
        .get("components")
        .and_then(Value::as_array)
        .ok_or_else(|| EconError {
            msg: "intent wire: explanation.components must be an array".to_string(),
        })?;
    let mut components = Vec::with_capacity(components_raw.len());
    for pair in components_raw {
        let arr = pair.as_array().ok_or_else(|| EconError {
            msg: "intent wire: explanation.components entries must be [name, value] pairs"
                .to_string(),
        })?;
        if arr.len() != 2 {
            return Err(EconError {
                msg: "intent wire: explanation.components entries must have exactly 2 elements"
                    .to_string(),
            });
        }
        let name = arr[0].as_str().ok_or_else(|| EconError {
            msg: "intent wire: explanation.components name must be a string".to_string(),
        })?;
        components.push((name.to_string(), arr[1].clone()));
    }

    let priority_raw = wire_i64(d, "priority")?;
    let priority: i32 = priority_raw.try_into().map_err(|_| EconError {
        msg: format!("intent wire: priority out of range: {priority_raw}"),
    })?;

    let env = IntentEnvelope {
        intent_id: IntentId::new(wire_str(d, "intent_id")?)?,
        intent_type: wire_str(d, "intent_type")?,
        idempotency_key: wire_str(d, "idempotency_key")?,
        snapshot_id: wire_str(d, "snapshot_id")?,
        created_at: UnixTime::new(wire_i64(d, "created_at")?)?,
        expires_at: UnixTime::new(wire_i64(d, "expires_at")?)?,
        target: wire_str(d, "target")?,
        amount_msat,
        expected_benefit_msat: SignedMsat(wire_i64(d, "expected_benefit_msat")?),
        max_cost_msat: Msat::new(wire_i64(d, "max_cost_msat")?)?,
        capital_committed_msat: Msat::new(wire_i64(d, "capital_committed_msat")?)?,
        confidence_micro: Micro::new(wire_i64(d, "confidence_micro")?)?,
        reason_codes: wire_str_array(d, "reason_codes")?,
        explanation: Explanation { kind, components },
        preconditions: wire_str_array(d, "preconditions")?,
        priority,
        budget_bucket: wire_str(d, "budget_bucket")?,
        origin_policy: wire_str(d, "origin_policy")?,
        reversible: wire_bool(d, "reversible")?,
        schema_name: SCHEMA_NAME.to_string(),
        schema_version: SCHEMA_VERSION,
    };
    env.validate()?;
    Ok(env)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intent_types_has_nine_entries_in_python_order() {
        assert_eq!(INTENT_TYPES.len(), 9);
        assert_eq!(INTENT_TYPES[0], "SET_FEE");
        assert_eq!(INTENT_TYPES[8], "MAINTAIN_ONCHAIN_RESERVE");
    }

    #[test]
    fn explanation_render_matches_python_str_semantics() {
        let e = Explanation {
            kind: "conformance".to_string(),
            components: vec![
                ("name".to_string(), Value::String("bob".to_string())),
                ("count".to_string(), json!(3)),
                ("flag".to_string(), Value::Bool(true)),
                ("flag2".to_string(), Value::Bool(false)),
                ("missing".to_string(), Value::Null),
            ],
        };
        assert_eq!(
            e.render(),
            "conformance: name=bob, count=3, flag=True, flag2=False, missing=None"
        );
    }

    #[test]
    #[should_panic(expected = "float component values are not supported")]
    fn explanation_render_panics_on_float_component() {
        let e = Explanation {
            kind: "k".to_string(),
            components: vec![("x".to_string(), json!(1.5))],
        };
        e.render();
    }

    // --- compute_idempotency_key / make_intent golden values ---
    //
    // Reconstructs the generator `_env()` defaults from
    // `tests/conformance/scenarios/31-duplicate-idempotency-key/case.json`
    // and `tests/conformance/scenarios/37-clock-seed-determinism/case.json`
    // (both cite the same underlying intent): REBALANCE, target
    // "111x222x0", amount 400_000 sats (400_000_000 msat), snapshot
    // "snap-1", bucket "rebalance". Both corpus files pin:
    //   idempotency_key = "1119af2f103eb34445d30decf78b97f16d11267a8fe66e6aa83b340abdbe33f4"
    //   intent_id       = "int-1119af2f103eb344"
    // Independently re-verified against the live Python reference:
    //   python3 -c "
    //   from modules.econ_intents import compute_idempotency_key
    //   print(compute_idempotency_key(intent_type='REBALANCE', target='111x222x0',
    //       amount_msat=400_000_000, snapshot_id='snap-1', budget_bucket='rebalance'))"
    //   => 1119af2f103eb34445d30decf78b97f16d11267a8fe66e6aa83b340abdbe33f4

    const GOLDEN_KEY: &str = "1119af2f103eb34445d30decf78b97f16d11267a8fe66e6aa83b340abdbe33f4";
    const GOLDEN_INTENT_ID: &str = "int-1119af2f103eb344";

    #[test]
    fn compute_idempotency_key_matches_corpus_golden_value() {
        let key = compute_idempotency_key(
            "REBALANCE",
            "111x222x0",
            Some(400_000_000),
            "snap-1",
            "rebalance",
        );
        assert_eq!(key, GOLDEN_KEY);
    }

    fn golden_fields() -> IntentFields {
        let created_at = UnixTime::new(1_752_400_000).unwrap();
        IntentFields {
            intent_type: "REBALANCE".to_string(),
            snapshot_id: "snap-1".to_string(),
            created_at,
            expires_at: created_at.plus_seconds(600).unwrap(),
            target: "111x222x0".to_string(),
            amount_msat: Some(Msat::new(400_000_000).unwrap()),
            expected_benefit_msat: SignedMsat(0),
            max_cost_msat: Msat::new(3_000_000).unwrap(),
            capital_committed_msat: Msat::new(400_000_000).unwrap(),
            confidence_micro: Micro::new(0).unwrap(),
            reason_codes: vec![],
            explanation: Explanation {
                kind: "conformance".to_string(),
                components: vec![("case".to_string(), json!(1))],
            },
            preconditions: vec![],
            priority: 50,
            budget_bucket: "rebalance".to_string(),
            origin_policy: "conformance".to_string(),
            reversible: false,
        }
    }

    #[test]
    fn make_intent_matches_corpus_golden_intent_id_and_key() {
        let env = make_intent(golden_fields()).unwrap();
        assert_eq!(env.idempotency_key, GOLDEN_KEY);
        assert_eq!(env.intent_id.as_str(), GOLDEN_INTENT_ID);
    }

    #[test]
    fn make_intent_is_deterministic_no_hidden_entropy() {
        // Workstream J (corpus s37): identical (fields, clock) -> identical
        // ids, twice in a row, from independently-built field sets.
        let a = make_intent(golden_fields()).unwrap();
        let b = make_intent(golden_fields()).unwrap();
        assert_eq!(a.intent_id, b.intent_id);
        assert_eq!(a.idempotency_key, b.idempotency_key);
    }

    #[test]
    fn make_intent_rejects_unknown_intent_type() {
        let mut f = golden_fields();
        f.intent_type = "NOT_A_TYPE".to_string();
        assert!(make_intent(f).is_err());
    }

    #[test]
    fn make_intent_rejects_empty_snapshot_or_target() {
        let mut f = golden_fields();
        f.snapshot_id = String::new();
        assert!(make_intent(f).is_err());

        let mut f2 = golden_fields();
        f2.target = String::new();
        assert!(make_intent(f2).is_err());
    }

    #[test]
    fn make_intent_rejects_empty_bucket_or_origin() {
        let mut f = golden_fields();
        f.budget_bucket = String::new();
        assert!(make_intent(f).is_err());

        let mut f2 = golden_fields();
        f2.origin_policy = String::new();
        assert!(make_intent(f2).is_err());
    }

    #[test]
    fn make_intent_rejects_expires_at_not_after_created_at() {
        let mut f = golden_fields();
        f.expires_at = f.created_at; // equal, not strictly after
        assert!(make_intent(f).is_err());
    }

    #[test]
    fn make_intent_rejects_priority_out_of_range() {
        let mut f = golden_fields();
        f.priority = -1;
        assert!(make_intent(f).is_err());

        let mut f2 = golden_fields();
        f2.priority = 101;
        assert!(make_intent(f2).is_err());
    }

    #[test]
    fn make_intent_rejects_unknown_reason_code() {
        let mut f = golden_fields();
        f.reason_codes = vec!["NOT_A_CODE".to_string()];
        assert!(make_intent(f).is_err());
    }

    #[test]
    fn make_intent_accepts_valid_reason_code() {
        let mut f = golden_fields();
        f.reason_codes = vec!["BUDGET_EXHAUSTED".to_string()];
        assert!(make_intent(f).is_ok());
    }

    #[test]
    fn make_intent_rejects_empty_explanation_kind() {
        let mut f = golden_fields();
        f.explanation.kind = String::new();
        let result = make_intent(f);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.msg.contains("Explanation.kind"),
            "error message should name the field: {}",
            err.msg
        );
    }

    #[test]
    fn make_intent_accepts_none_amount() {
        let mut f = golden_fields();
        f.amount_msat = None;
        let env = make_intent(f).unwrap();
        assert!(env.amount_msat.is_none());
        // A different amount_msat (None vs Some) must not collide with the
        // golden key.
        assert_ne!(env.idempotency_key, GOLDEN_KEY);
    }

    // --- is_expired (corpus s34: 34-expired-intent) ---

    #[test]
    fn is_expired_inclusive_boundary_matches_corpus_s34() {
        let env = make_intent(golden_fields()).unwrap();
        assert_eq!(env.expires_at.value(), 1_752_400_600);
        let probes = [1_752_400_599i64, 1_752_400_600, 1_752_400_601];
        let results: Vec<bool> = probes
            .iter()
            .map(|&t| is_expired(&env, UnixTime::new(t).unwrap()))
            .collect();
        assert_eq!(results, vec![false, true, true]);
    }

    // --- wire round-trip ---

    #[test]
    fn to_wire_from_wire_round_trips() {
        let env = make_intent(golden_fields()).unwrap();
        let wire = to_wire(&env);
        let round_tripped = from_wire(&wire).unwrap();
        assert_eq!(env, round_tripped);
    }

    #[test]
    fn to_wire_shape_matches_python_field_names() {
        let env = make_intent(golden_fields()).unwrap();
        let wire = to_wire(&env);
        let obj = wire.as_object().unwrap();
        for key in [
            "schema_name",
            "schema_version",
            "intent_id",
            "intent_type",
            "idempotency_key",
            "snapshot_id",
            "created_at",
            "expires_at",
            "target",
            "amount_msat",
            "expected_benefit_msat",
            "max_cost_msat",
            "capital_committed_msat",
            "confidence_micro",
            "reason_codes",
            "explanation",
            "preconditions",
            "priority",
            "budget_bucket",
            "origin_policy",
            "reversible",
        ] {
            assert!(obj.contains_key(key), "missing wire field {key}");
        }
        let components = wire["explanation"]["components"].as_array().unwrap();
        assert_eq!(components[0], json!(["case", 1]));
    }

    #[test]
    fn from_wire_rejects_schema_mismatch() {
        let env = make_intent(golden_fields()).unwrap();
        let mut wire = to_wire(&env);
        wire["schema_name"] = json!("not-intent");
        assert!(from_wire(&wire).is_err());

        let mut wire2 = to_wire(&env);
        wire2["schema_version"] = json!(1);
        assert!(from_wire(&wire2).is_err());
    }

    #[test]
    fn from_wire_rejects_missing_field() {
        let env = make_intent(golden_fields()).unwrap();
        let mut wire = to_wire(&env);
        wire.as_object_mut().unwrap().remove("target");
        assert!(from_wire(&wire).is_err());
    }

    #[test]
    fn from_wire_none_amount_round_trips() {
        let mut f = golden_fields();
        f.amount_msat = None;
        let env = make_intent(f).unwrap();
        let wire = to_wire(&env);
        assert!(wire["amount_msat"].is_null());
        let round_tripped = from_wire(&wire).unwrap();
        assert_eq!(env, round_tripped);
    }

    #[test]
    fn from_wire_rejects_empty_explanation_kind() {
        let env = make_intent(golden_fields()).unwrap();
        let mut wire = to_wire(&env);
        wire["explanation"]["kind"] = json!("");
        let result = from_wire(&wire);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.msg.contains("Explanation.kind"),
            "error message should name the field: {}",
            err.msg
        );
    }
}
