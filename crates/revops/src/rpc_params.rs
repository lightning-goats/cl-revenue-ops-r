//! Shared param-coercion helpers for Wave-2 RPC Batch A's ten handlers
//! (Task 50 correction round).
//!
//! Two cross-cutting problems the Task 50 audit found across ALL ten
//! handlers, fixed once here instead of ad-hoc per handler:
//!
//! 1. **Positional params.** `cln-plugin` hands each `.rpcmethod()` closure
//!    CLN's raw JSON `params` value verbatim. pyln (the Python side) binds
//!    a JSON ARRAY positionally by argument order; none of Batch A's Rust
//!    handlers implement that, so `v.get("name")` on an array silently
//!    returns `None` and every param falls back to its default -- the
//!    audit's example: `lightning-cli revenue-r-spend-ledger 48` running a
//!    24h window instead of 48h, with no error at all. [`reject_positional_params`]
//!    closes that specific hole (Batch-A-only, not a port-wide positional
//!    binder -- that's a separate, larger follow-up).
//!
//! 2. **Python's `int(x)`/truthiness coercions.** `revenue-r-policy`'s
//!    `since` and `revenue-r-spend-ledger`'s `window_hours`/
//!    `reservation_limit` all go through Python's `int(x)` (raises
//!    `ValueError`/`TypeError` with a specific message on garbage) and
//!    `include_reservations` goes through Python's `bool(x)` truthiness
//!    (`bool("false") is True`). [`python_int`] and [`is_truthy_py`] give
//!    every caller in this crate the SAME coercion instead of each
//!    reinventing (and subtly diverging on) it.

use serde::Deserialize;
use serde_json::{Map, Value};
use std::fmt;

/// Binding policy is an explicit call-site choice. The checked-in Python
/// contract records `PositionalOrNamed`; a still-incomplete Rust handler may
/// deliberately choose `NamedOnly`, which produces a typed refusal before its
/// closure runs instead of silently defaulting positional arguments.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParamBinding {
    PositionalOrNamed,
    NamedOnly,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParamCoercion {
    /// Signature extraction cannot truthfully infer coercion performed inside
    /// a Python handler. Preserve the JSON value for the handler-specific port.
    HandlerDefined,
    String,
    PythonInt,
    Number,
    Boolean,
    PythonTruthy,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ParamSpec {
    pub name: String,
    pub required: bool,
    pub has_default: bool,
    pub default: Value,
    pub coercion: ParamCoercion,
    /// `None` is accepted for small synthetic test contracts. Generated
    /// contracts always set this to `positional` or `keyword_only`.
    #[serde(default)]
    pub python_kind: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct RpcMethodSpec {
    pub name: String,
    pub python_binding: ParamBinding,
    #[serde(default)]
    pub allow_extra_named: bool,
    pub params: Vec<ParamSpec>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct RpcParameterContract {
    pub schema_version: u32,
    pub python_source_commit: String,
    pub python_source_sha256: String,
    pub methods: Vec<RpcMethodSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParamDecodeError {
    InvalidShape { actual: &'static str },
    NamedOnly,
    ExcessPositional { expected: usize, actual: usize },
    UnknownNamed { name: String },
    MissingRequired { name: String },
    Coercion { name: String, message: String },
}

impl fmt::Display for ParamDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidShape { actual } => {
                write!(
                    f,
                    "RPC params must be an object, array, or null; got {actual}"
                )
            }
            Self::NamedOnly => write!(
                f,
                "positional parameters are not supported; use named parameters"
            ),
            Self::ExcessPositional { expected, actual } => write!(
                f,
                "too many positional parameters: expected at most {expected}, got {actual}"
            ),
            Self::UnknownNamed { name } => write!(f, "unknown named parameter: {name}"),
            Self::MissingRequired { name } => write!(f, "missing required parameter: {name}"),
            Self::Coercion { name, message } => {
                write!(f, "invalid parameter {name}: {message}")
            }
        }
    }
}

impl std::error::Error for ParamDecodeError {}

pub fn load_rpc_contract() -> RpcParameterContract {
    serde_json::from_str(include_str!("../../../fixtures/port/rpc_params.json"))
        .expect("embedded RPC parameter contract is valid")
}

fn value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn coerce(spec: &ParamSpec, value: &Value) -> Result<Value, ParamDecodeError> {
    let failed = |message: String| ParamDecodeError::Coercion {
        name: spec.name.clone(),
        message,
    };
    match spec.coercion {
        ParamCoercion::HandlerDefined => Ok(value.clone()),
        ParamCoercion::String => value
            .as_str()
            .map(|text| Value::String(text.to_string()))
            .ok_or_else(|| failed(format!("expected string, got {}", value_kind(value)))),
        ParamCoercion::PythonInt => python_int(value)
            .map(|number| Value::Number(number.into()))
            .map_err(failed),
        ParamCoercion::Number => match value {
            Value::Number(_) => Ok(value.clone()),
            _ => Err(failed(format!(
                "expected number, got {}",
                value_kind(value)
            ))),
        },
        ParamCoercion::Boolean => value
            .as_bool()
            .map(Value::Bool)
            .ok_or_else(|| failed(format!("expected boolean, got {}", value_kind(value)))),
        ParamCoercion::PythonTruthy => Ok(Value::Bool(is_truthy_py(value))),
    }
}

/// Decode raw CLN JSON params into one named map. Every refusal is returned
/// before a handler can see partially bound/defaulted values.
pub fn decode_params(
    spec: &RpcMethodSpec,
    raw: &Value,
    binding: ParamBinding,
) -> Result<Map<String, Value>, ParamDecodeError> {
    let mut supplied = Map::new();
    match raw {
        Value::Null => {}
        Value::Object(values) => {
            for (name, value) in values {
                if !spec.allow_extra_named && !spec.params.iter().any(|param| param.name == *name) {
                    return Err(ParamDecodeError::UnknownNamed { name: name.clone() });
                }
                supplied.insert(name.clone(), value.clone());
            }
        }
        Value::Array(values) => {
            if binding == ParamBinding::NamedOnly && !values.is_empty() {
                return Err(ParamDecodeError::NamedOnly);
            }
            let positional = spec
                .params
                .iter()
                .filter(|param| param.python_kind.as_deref() != Some("keyword_only"))
                .collect::<Vec<_>>();
            if values.len() > positional.len() {
                return Err(ParamDecodeError::ExcessPositional {
                    expected: positional.len(),
                    actual: values.len(),
                });
            }
            for (value, param) in values.iter().zip(positional) {
                supplied.insert(param.name.clone(), value.clone());
            }
        }
        scalar => {
            return Err(ParamDecodeError::InvalidShape {
                actual: value_kind(scalar),
            });
        }
    }

    let mut decoded = Map::new();
    for param in &spec.params {
        if let Some(value) = supplied.remove(&param.name) {
            decoded.insert(param.name.clone(), coerce(param, &value)?);
        } else if param.has_default {
            decoded.insert(param.name.clone(), coerce(param, &param.default)?);
        } else if param.required {
            return Err(ParamDecodeError::MissingRequired {
                name: param.name.clone(),
            });
        }
    }
    // Only a Python **kwargs contract can leave names here.
    decoded.extend(supplied);
    Ok(decoded)
}

pub fn decode_and_call<R>(
    spec: &RpcMethodSpec,
    raw: &Value,
    binding: ParamBinding,
    handler: impl FnOnce(Map<String, Value>) -> R,
) -> Result<R, ParamDecodeError> {
    let decoded = decode_params(spec, raw, binding)?;
    Ok(handler(decoded))
}

/// Batch A's params-shape gate. `lightning-cli`'s own no-argument call
/// shape is an EMPTY JSON array (`[]`), not `{}` -- that must still mean
/// "no params" (same as an absent/empty object), so only a NON-EMPTY array
/// is refused. Returns `Some(error)` when the caller should stop and return
/// that value as-is; `None` means "proceed as normal" (object, empty array,
/// or any other JSON shape -- `v.get("name")` on those already degrades to
/// `None`/absent exactly like Python's missing-kwarg case).
pub fn reject_positional_params(v: &Value) -> Option<Value> {
    if let Value::Array(items) = v {
        if !items.is_empty() {
            return Some(serde_json::json!({
                "error": "positional parameters are not supported by this port; \
                          use named parameters (a JSON object), e.g. \
                          {\"window_hours\": 48}"
            }));
        }
    }
    None
}

/// Python truthiness (`bool(x)`/`if x`) over a JSON value: `None`/`null` is
/// falsy; numeric zero (int or float) is falsy; the empty string, empty
/// array, and empty object are falsy; everything else is truthy. Matches
/// CPython's `__bool__`/`__len__` fallback rules for the JSON-representable
/// subset of Python types Batch A's params can arrive as.
pub fn is_truthy_py(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i != 0
            } else if let Some(u) = n.as_u64() {
                u != 0
            } else if let Some(f) = n.as_f64() {
                f != 0.0
            } else {
                true
            }
        }
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// Round-2 correction, P1: the exclusive upper bound of `i64`'s range as an
/// `f64`, `2^63`. Both `i64::MIN as f64` (`-2^63`, exactly representable)
/// and this constant are exact powers of two, so comparing a truncated
/// float against them is exact (no rounding ambiguity at the boundary) --
/// unlike comparing against `i64::MAX as f64`, which itself rounds UP to
/// `2^63` (one past the real max) and would let `9223372036854775808.0`
/// wrongly pass an `<=` check, then saturate on the `as i64` cast.
const I64_UPPER_BOUND_F64: f64 = 9_223_372_036_854_775_808.0;

/// A faithful-enough port of Python's `int(x)` coercion over a JSON value,
/// matching CPython's exact exception text for the shapes Batch A's params
/// can take: `Ok(n)` on success, `Err(message)` with the SAME string
/// `str(e)` would produce in Python (used verbatim in Batch A's in-band
/// `{"error": str(e)}` responses) for the shapes Python itself can raise
/// on; a DIFFERENT, Rust-specific message for the one shape Python never
/// raises on at all (see below).
///
/// - number: `as_i64` exactly; a float truncates toward zero (`48.9` ->
///   `48`, `-48.9` -> `-48`), matching Python's `int(float)`.
/// - bool: `int(True) == 1`, `int(False) == 0`.
/// - string: trimmed, then parsed as a plain base-10 integer (Python's
///   `int(str)` accepts leading/trailing whitespace and an optional sign,
///   but NOT a decimal point) -- `"48"` -> `48`, `" 48 "` -> `48`,
///   `"48.9"` -> the `ValueError` text below (Python's `int()` never
///   accepts a fractional string, only a fractional NUMBER).
/// - null/array/object: Python's `TypeError` text (`int(None)`,
///   `int([])`, `int({})` all raise this, regardless of contents).
///
/// **Round-2 correction, P1: an unsigned integer or float outside `i64`'s
/// representable range is now a LOUD `Err`, never a silent wrap/saturate.**
/// Python's `int` is arbitrary-precision -- `int(18446744073709551615)`
/// never fails in Python, so there is no Python exception TEXT to port for
/// this case. The OLD Rust code instead let the underlying integer cast do
/// whatever it does by default: `u as i64` on a `u64` bigger than
/// `i64::MAX` WRAPS (`u64::MAX as i64 == -1`), and `f.trunc() as i64` on an
/// out-of-range float SATURATES (Rust's defined float->int cast behavior
/// since 1.45, clamping to `i64::MAX`/`i64::MIN`) -- both silently produce
/// a DIFFERENT, WRONG `i64` instead of failing, and every caller
/// (`parse_window_hours`, `parse_reservation_limit`, `coerce_since`) then
/// ran a successful-looking query against that wrong value (the audit's
/// example: `u64::MAX` wrapping to `-1`, then `.max(1)` turning it into a
/// confident 1-hour spend ledger). An explicit range error here is
/// correct, not a parity gap: these Rust query interfaces (`i64` window
/// hours, `i64` limits, `i64` unix timestamps) cannot represent a value
/// outside `i64`'s range faithfully, so refusing loudly is the only choice
/// that doesn't silently substitute a different, incorrect answer.
pub fn python_int(v: &Value) -> Result<i64, String> {
    match v {
        Value::Bool(b) => Ok(if *b { 1 } else { 0 }),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i)
            } else if let Some(u) = n.as_u64() {
                i64::try_from(u).map_err(|_| {
                    format!(
                        "value {u} is outside the range this operation can represent \
                         (Python's arbitrary-precision int has no i64-range limit here, \
                         but this Rust query interface does)"
                    )
                })
            } else if let Some(f) = n.as_f64() {
                if !f.is_finite() {
                    return Err(format!("cannot convert non-finite float {f} to an integer"));
                }
                let truncated = f.trunc();
                if truncated >= i64::MIN as f64 && truncated < I64_UPPER_BOUND_F64 {
                    Ok(truncated as i64)
                } else {
                    Err(format!(
                        "value {f} is outside the range this operation can represent \
                         (Python's arbitrary-precision int has no i64-range limit here, \
                         but this Rust query interface does)"
                    ))
                }
            } else {
                Err("int() argument out of range".to_string())
            }
        }
        Value::String(s) => s
            .trim()
            .parse::<i64>()
            .map_err(|_| format!("invalid literal for int() with base 10: '{s}'")),
        Value::Null => Err(
            "int() argument must be a string, a bytes-like object or a real number, \
             not 'NoneType'"
                .to_string(),
        ),
        Value::Array(_) => Err(
            "int() argument must be a string, a bytes-like object or a real number, \
             not 'list'"
                .to_string(),
        ),
        Value::Object(_) => Err(
            "int() argument must be a string, a bytes-like object or a real number, \
             not 'dict'"
                .to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reject_positional_params_refuses_nonempty_array() {
        let err = reject_positional_params(&json!([48])).expect("must refuse");
        assert!(err["error"]
            .as_str()
            .unwrap()
            .contains("positional parameters are not supported"));
    }

    #[test]
    fn reject_positional_params_allows_empty_array_as_no_params() {
        assert!(reject_positional_params(&json!([])).is_none());
    }

    #[test]
    fn reject_positional_params_allows_object() {
        assert!(reject_positional_params(&json!({"window_hours": 48})).is_none());
    }

    #[test]
    fn is_truthy_py_matches_python_falsy_set() {
        assert!(!is_truthy_py(&json!(null)));
        assert!(!is_truthy_py(&json!(false)));
        assert!(!is_truthy_py(&json!(0)));
        assert!(!is_truthy_py(&json!(0.0)));
        assert!(!is_truthy_py(&json!("")));
        assert!(!is_truthy_py(&json!([])));
        assert!(!is_truthy_py(&json!({})));
    }

    #[test]
    fn is_truthy_py_string_false_is_truthy() {
        // The exact quirk the audit calls out: `bool("false")` is `True`.
        assert!(is_truthy_py(&json!("false")));
        assert!(is_truthy_py(&json!("0")));
    }

    #[test]
    fn python_int_accepts_numbers_bools_and_numeric_strings() {
        assert_eq!(python_int(&json!(48)), Ok(48));
        assert_eq!(python_int(&json!(48.9)), Ok(48));
        assert_eq!(python_int(&json!(-48.9)), Ok(-48));
        assert_eq!(python_int(&json!(true)), Ok(1));
        assert_eq!(python_int(&json!(false)), Ok(0));
        assert_eq!(python_int(&json!("48")), Ok(48));
        assert_eq!(python_int(&json!(" 48 ")), Ok(48));
        assert_eq!(python_int(&json!("-5")), Ok(-5));
    }

    /// Round-2 correction, P1: a JSON unsigned integer outside `i64`'s
    /// range (`u64::MAX`) must be a LOUD in-band error, never wrap. The
    /// OLD `u as i64` cast wrapped `18446744073709551615` to `-1` --
    /// `parse_window_hours(...).max(1)` then ran a successful-looking
    /// 1-hour ledger for what should have been an outright-rejected
    /// request. Python's `int()` never has this problem (arbitrary
    /// precision); an explicit range error is the correct Rust answer
    /// since these query interfaces cannot represent the value faithfully
    /// -- a DIFFERENT wrong value succeeding silently is not acceptable.
    #[test]
    fn python_int_rejects_u64_max_loudly_instead_of_wrapping() {
        let err = python_int(&json!(u64::MAX)).expect_err("must not wrap to -1");
        assert!(
            !err.is_empty(),
            "must carry a non-empty in-band error message"
        );
    }

    /// Same requirement, the float path: `f.trunc() as i64` on an
    /// out-of-i64-range float SATURATES (Rust's float->int cast behavior
    /// since 1.45) rather than panicking or erroring -- a huge float would
    /// silently become `i64::MAX`/`i64::MIN`, a different wrong value
    /// succeeding instead of erroring.
    #[test]
    fn python_int_rejects_out_of_range_float_loudly_instead_of_saturating() {
        let err = python_int(&json!(1e20)).expect_err("must not saturate to i64::MAX");
        assert!(!err.is_empty());
        let err_min = python_int(&json!(-1e20)).expect_err("must not saturate to i64::MIN");
        assert!(!err_min.is_empty());
    }

    /// In-range values on both paths must still succeed exactly as before
    /// -- the range check must not become an over-broad rejection.
    #[test]
    fn python_int_still_accepts_in_range_u64_and_float() {
        assert_eq!(python_int(&json!(i64::MAX as u64)), Ok(i64::MAX));
        assert_eq!(python_int(&json!(1e10)), Ok(10_000_000_000));
    }

    #[test]
    fn python_int_rejects_garbage_with_python_style_messages() {
        assert_eq!(
            python_int(&json!("abc")),
            Err("invalid literal for int() with base 10: 'abc'".to_string())
        );
        assert_eq!(
            python_int(&json!("48.9")),
            Err("invalid literal for int() with base 10: '48.9'".to_string())
        );
        assert!(python_int(&json!(null)).is_err());
        assert!(python_int(&json!([])).is_err());
        assert!(python_int(&json!({})).is_err());
    }
}
