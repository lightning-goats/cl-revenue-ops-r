use serde_json::Value;
use thiserror::Error;

/// Byte-parity with json.dumps(sort_keys=True, separators=(",",":"),
/// ensure_ascii=False). Escapes only what Python escapes: " \\ and control
/// chars (as \n \t \r \b \f or \u00XX); non-ASCII passes through raw.
///
/// **Integer-only contract**: canonical payloads in this system are
/// integer-only by design (the governed econ layer uses checked integer
/// money math — no floats). Any `Value::Number` that is float-typed
/// (`serde_json::Number::is_f64()`, which includes integral floats like
/// `1.0` since serde_json preserves how a number was constructed) is
/// rejected with `Err`, never rendered.
///
/// This is fail-closed by design: Python's `repr`/`json.dumps` float
/// formatting and Rust's `f64::to_string()` diverge for values like `1e-5`
/// (Python: `"1e-05"`, Rust: `"0.00001"`). A float silently forwarded here
/// would render differently in Rust than in Python, byte-parity would
/// break, and this function feeds idempotency keys — so a divergent float
/// would silently produce a different idempotency key with no test
/// failing. Rejecting all floats outright removes that failure mode
/// entirely rather than trying to reimplement Python's float repr.
pub fn canonical_json(v: &Value) -> Result<String, CanonicalError> {
    let mut out = String::new();
    let mut path = Vec::new();
    write_value(v, &mut out, &mut path)?;
    Ok(out)
}

/// Error returned by [`canonical_json`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CanonicalError {
    /// A float-typed `Value::Number` was encountered. `path` is a
    /// JSON-pointer-ish location of the offending value, e.g. `/a/2/rate`.
    #[error("canonical JSON forbids non-integer numbers (at {path})")]
    NonIntegerNumber { path: String },
}

fn write_value(v: &Value, out: &mut String, path: &mut Vec<String>) -> Result<(), CanonicalError> {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(n) => {
            if n.is_f64() {
                return Err(CanonicalError::NonIntegerNumber {
                    path: format_path(path),
                });
            }
            out.push_str(&n.to_string());
        }
        Value::String(s) => write_string(s, out),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                path.push(i.to_string());
                let result = write_value(item, out, path);
                path.pop();
                result?;
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
                write_string(k, out);
                out.push(':');
                path.push((*k).clone());
                let result = write_value(&map[*k], out, path);
                path.pop();
                result?;
            }
            out.push('}');
        }
    }
    Ok(())
}

/// Renders the accumulated path segments as a JSON-pointer-ish string, e.g.
/// `["a", "1"]` -> `/a/1`. The root path (no segments) renders as `/`.
fn format_path(path: &[String]) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    let mut s = String::new();
    for seg in path {
        s.push('/');
        s.push_str(seg);
    }
    s
}

fn write_string(s: &str, out: &mut String) {
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
