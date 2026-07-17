#![forbid(unsafe_code)]

//! Library surface for the `revops` plugin binary: the pieces worth
//! unit-testing in isolation from the cln-plugin stdio handshake (which the
//! `tests/manifest.rs` black-box test covers instead).

pub mod config_resolve;
pub mod config_types;
pub mod fee_evidence;
pub mod hydration;
pub mod notify;
pub mod options_table;
pub mod rpc_dashboard;
pub mod rpc_history;
pub mod rpc_report;
pub mod rpc_status;

/// Current Unix time in whole seconds, matching Python's
/// `int(time.time())` as used throughout `database.py`/
/// `profitability_analyzer.py`'s windowed queries. A thin wrapper so the
/// read-RPC handlers in `main.rs` don't each repeat the
/// `SystemTime`/`UNIX_EPOCH` dance; returns `0` on a pre-epoch clock rather
/// than panicking mid-request.
pub fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// `serde_json::Value` -> `String`, for `opt_type == "string"` defaults.
pub fn as_string_default(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        other => Some(other.to_string()),
    }
}

/// `serde_json::Value` -> `i64`, for `opt_type == "int"` defaults. The
/// Python source stores every default as a string literal (even for the one
/// `opt_type="int"` option), so this accepts both a JSON number and a
/// numeric string.
pub fn as_int_default(v: &serde_json::Value) -> Option<i64> {
    match v {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

/// `serde_json::Value` -> `bool`, for `opt_type == "bool"` defaults.
pub fn as_bool_default(v: &serde_json::Value) -> Option<bool> {
    match v {
        serde_json::Value::Bool(b) => Some(*b),
        serde_json::Value::Number(n) => n.as_i64().map(|i| i != 0),
        serde_json::Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => None,
        },
        _ => None,
    }
}
