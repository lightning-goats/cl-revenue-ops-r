//! Frozen datastore telemetry byte-shapes: the
//! `["revenue", "profitability-summary"]` payload and the `datastore_push`
//! envelope rules (port of `modules/profitability_analyzer.py`'s
//! `_push_profitability_summary` + `modules/data_service.py::datastore_push`).
//!
//! ## THE trap (Global Constraints #2 in the phase plan)
//!
//! Python's `json.dumps(payload)` with its DEFAULT arguments is a THIRD,
//! distinct serializer alongside `revops_core::canonical::canonical_json`
//! (sorted, compact, float-rejecting) and
//! `revops_econ::ledger::python_dumps_default` (`sort_keys=True`, default
//! separators, float-rejecting):
//!
//! - keys in **insertion order** (dict literal / construction order — NOT
//!   sorted);
//! - separators `(", ", ": ")` (Python's non-compact default, same as the
//!   other two);
//! - `ensure_ascii=True` (non-ASCII escaped as `\uXXXX`, with UTF-16
//!   surrogate pairs above the BMP) — the opposite of
//!   `canonical_json`'s `ensure_ascii=False`;
//! - floats rendered via CPython's `repr(float)` (`revops_econ::pyfloat::py_repr`),
//!   not rejected — this payload has real float fields (`roi_pct`,
//!   `fee_multiplier`).
//!
//! [`PyDict`] is backed by an insertion-ordered `Vec<(String, PyVal)>` —
//! never a sorted map (a `BTreeMap`/`HashMap`-backed dict would silently
//! reorder keys and produce byte-different output).
//!
//! ## Task 8 (Wave 2) of
//! `docs/superpowers/plans/2026-07-17-phase3-analytics-budget.md`.

use revops_econ::pyfloat::{py_repr, py_round};

use crate::profitability::ChannelProfitability;

// =============================================================================
// PyVal / PyDict: the insertion-ordered value model for `json.dumps` DEFAULTS
// =============================================================================

/// A JSON-encodable value in Python's `json.dumps` default wire shape.
/// Deliberately narrow — only the variants this payload (and its envelope)
/// need; not a general-purpose JSON value type (use `serde_json::Value` /
/// `revops_core::canonical` for that).
#[derive(Debug, Clone, PartialEq)]
pub enum PyVal {
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Dict(PyDict),
}

/// An insertion-ordered dict: Python dict semantics (construction order
/// preserved, `json.dumps` walks keys in that order — never sorted).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PyDict(Vec<(String, PyVal)>);

impl PyDict {
    pub fn new() -> Self {
        PyDict(Vec::new())
    }

    /// Append a key/value pair. Mirrors Python's `d[k] = v` for a key that
    /// isn't already present (this type never dedupes/overwrites — callers
    /// are expected to only ever construct fresh dicts key-by-key, matching
    /// the dict-literal shape of the ported Python).
    pub fn push(&mut self, key: impl Into<String>, value: PyVal) {
        self.0.push((key.into(), value));
    }

    /// `key in d` — Python's `in` operator for dict keys.
    pub fn contains_key(&self, key: &str) -> bool {
        self.0.iter().any(|(k, _)| k == key)
    }

    pub fn iter(&self) -> impl Iterator<Item = &(String, PyVal)> {
        self.0.iter()
    }
}

/// Python `json.dumps(v)` with DEFAULT arguments: insertion-order keys,
/// separators `(", ", ": ")`, `ensure_ascii=True`, floats via `py_repr`.
pub fn python_dumps(v: &PyVal) -> String {
    let mut out = String::new();
    write_val(v, &mut out);
    out
}

fn write_val(v: &PyVal, out: &mut String) {
    match v {
        PyVal::Bool(true) => out.push_str("true"),
        PyVal::Bool(false) => out.push_str("false"),
        PyVal::Int(n) => out.push_str(&n.to_string()),
        PyVal::Float(f) => out.push_str(&py_repr(*f)),
        PyVal::Str(s) => write_str(s, out),
        PyVal::Dict(d) => write_dict(d, out),
    }
}

fn write_dict(d: &PyDict, out: &mut String) {
    out.push('{');
    for (i, (k, v)) in d.0.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        write_str(k, out);
        out.push_str(": ");
        write_val(v, out);
    }
    out.push('}');
}

/// `ensure_ascii=True` string encoding: every codepoint outside printable
/// ASCII (`< 0x20` or `>= 0x7F`) is escaped as `\uXXXX`, astral codepoints
/// as a UTF-16 surrogate pair (same rules as
/// `revops_econ::ledger::python_dumps_default`'s `write_string`, duplicated
/// here rather than shared since that helper is crate-private to
/// `revops-econ` and the two serializers are deliberately independent — see
/// the module doc comment's "THE trap" note).
fn write_str(s: &str, out: &mut String) {
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
            c if (c as u32) < 0x20 || (c as u32) >= 0x7F => write_unicode_escape(c, out),
            c => out.push(c),
        }
    }
    out.push('"');
}

fn write_unicode_escape(c: char, out: &mut String) {
    let cp = c as u32;
    if cp > 0xFFFF {
        let v = cp - 0x10000;
        let high = 0xD800 + (v >> 10);
        let low = 0xDC00 + (v & 0x3FF);
        out.push_str(&format!("\\u{high:04x}\\u{low:04x}"));
    } else {
        out.push_str(&format!("\\u{cp:04x}"));
    }
}

// =============================================================================
// The `["revenue", "profitability-summary"]` payload
// =============================================================================

/// The CLN datastore key path this payload is pushed under (`modules/
/// profitability_analyzer.py::_push_profitability_summary`'s
/// `self.data_service.datastore_push(["revenue", "profitability-summary"], payload)`).
pub const PROFITABILITY_SUMMARY_KEY: [&str; 2] = ["revenue", "profitability-summary"];

/// One channel's contribution to the `profitability-summary` payload: a
/// `ChannelProfitability` (Task 4's merged profitability builder — carries
/// every field the datastore entry reads except `fee_multiplier`) plus the
/// externally-computed fee multiplier.
///
/// `fee_multiplier` is INJECTED rather than computed here: in Python it
/// comes from `self.get_fee_multiplier(ch_id)`, a whole fee-stack decision
/// that is Phase 4 evidence (out of scope for this task's pure byte-shape
/// contract).
pub struct SummaryEntry<'a> {
    pub profitability: &'a ChannelProfitability,
    pub fee_multiplier: f64,
}

/// Build one channel's entry dict: the EXACT 29-key insertion order of
/// `_push_profitability_summary` (`modules/profitability_analyzer.py` lines
/// ~700-733). Every field ported verbatim; see the module's
/// `SummaryEntry` doc comment for the one injected value.
fn channel_entry_dict(entry: &SummaryEntry<'_>) -> PyDict {
    let p = entry.profitability;
    let revenue = &p.revenue;
    let costs = &p.costs;
    let mut d = PyDict::new();

    d.push("channel_id", PyVal::Str(p.channel_id.clone()));
    d.push("peer_id", PyVal::Str(p.peer_id.clone()));
    d.push("class", PyVal::Str(p.classification.as_value().to_string()));
    d.push("net_profit_sats", PyVal::Int(p.net_profit_sats));
    d.push("roi_pct", PyVal::Float(py_round(p.roi_percent, 2)));
    d.push("days_open", PyVal::Int(p.days_open));
    d.push("role", PyVal::Str(p.channel_role().as_value().to_string()));
    d.push(
        "fee_multiplier",
        PyVal::Float(py_round(entry.fee_multiplier, 2)),
    );
    d.push("forward_count", PyVal::Int(revenue.forward_count));
    d.push(
        "sourced_forward_count",
        PyVal::Int(revenue.sourced_forward_count),
    );
    d.push(
        "total_forward_count",
        PyVal::Int(revenue.total_forward_count()),
    );
    d.push("fees_earned_msat", PyVal::Int(revenue.fees_earned_msat));
    d.push("fees_earned_sats", PyVal::Int(revenue.fees_earned_sats()));
    d.push("volume_routed_msat", PyVal::Int(revenue.volume_routed_msat));
    d.push(
        "sourced_volume_msat",
        PyVal::Int(revenue.sourced_volume_msat),
    );
    d.push(
        "sourced_fee_contribution_msat",
        PyVal::Int(revenue.sourced_fee_contribution_msat),
    );
    d.push(
        "sourced_fee_contribution_sats",
        PyVal::Int(revenue.sourced_fee_contribution_sats()),
    );
    d.push(
        "total_contribution_msat",
        PyVal::Int(revenue.total_contribution_msat()),
    );
    d.push(
        "total_contribution_sats",
        PyVal::Int(revenue.total_contribution_sats()),
    );
    // sats -> msat dual-column convention: costs are stored sats-native and
    // PROMOTED to msat here (never "normalize" the other way).
    d.push("open_cost_msat", PyVal::Int(costs.open_cost_sats * 1000));
    d.push(
        "rebalance_cost_msat",
        PyVal::Int(costs.rebalance_cost_sats * 1000),
    );
    // Mixed-unit expression, ported verbatim (msat revenue minus a
    // sats-native cost promoted to msat inline):
    // `p.revenue.total_contribution_msat - sats_to_base(p.costs.total_cost_sats)`.
    d.push(
        "net_pnl_msat",
        PyVal::Int(revenue.total_contribution_msat() - costs.total_cost_sats() * 1000),
    );
    d.push("contribution_30d_msat", PyVal::Int(p.contribution_30d_msat));
    d.push("fees_earned_30d_msat", PyVal::Int(p.fees_earned_30d_msat));
    d.push("sourced_fee_30d_msat", PyVal::Int(p.sourced_fee_30d_msat));
    d.push("forward_count_30d", PyVal::Int(p.forward_count_30d));
    d.push(
        "sourced_forward_count_30d",
        PyVal::Int(p.sourced_forward_count_30d),
    );
    d.push("role_30d", PyVal::Str(p.role_30d().as_value().to_string()));
    d.push(
        "marginal_roi_reliable",
        PyVal::Bool(p.marginal_roi_reliable()),
    );

    d
}

/// Build the full `["revenue", "profitability-summary"]` payload dict —
/// `{"timestamp": ..., "channels": {...}}`, `channels` keyed by
/// `channel_id` in the exact order `entries` is given (mirrors Python
/// dict-insertion order from iterating `results.items()`).
pub fn profitability_summary_payload(entries: &[SummaryEntry<'_>], timestamp: i64) -> PyDict {
    let mut channels = PyDict::new();
    for entry in entries {
        let ch_id = entry.profitability.channel_id.clone();
        channels.push(ch_id, PyVal::Dict(channel_entry_dict(entry)));
    }

    let mut payload = PyDict::new();
    payload.push("timestamp", PyVal::Int(timestamp));
    payload.push("channels", PyVal::Dict(channels));
    payload
}

// =============================================================================
// datastore_push envelope decision
// =============================================================================

/// Error outcomes of the `datastore_push` envelope decision (port of
/// `modules/data_service.py::DataService.datastore_push`'s reject paths).
/// The actual CLN RPC call (`self._plugin.rpc.datastore(...)`) is transport
/// wiring, deferred to Phase 3b — this type only carries the pure decision.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TelemetryError {
    /// Python: `if "error" in payload: return False`.
    #[error("datastore payload contains a reserved \"error\" key")]
    ErrorKeyPresent,
    /// Python: `if encoded_bytes > self._DATASTORE_MAX_BYTES: ... return False`.
    #[error("datastore payload too large: {bytes} bytes exceeds cap of {max_bytes}")]
    TooLarge { bytes: usize, max_bytes: usize },
}

/// The `datastore_push` envelope decision (port of `modules/
/// data_service.py::DataService.datastore_push`, transport calls excluded —
/// see [`TelemetryError`]). Branch order matches the Python source exactly
/// (the `isinstance(payload, dict)` guard is enforced by Rust's type system
/// instead of a runtime check, since `payload: PyDict` can never be
/// anything else):
///
/// 1. `"error"` key present at the top level -> reject.
/// 2. `"timestamp"` key ABSENT at the top level -> append `("timestamp", now)`
///    (Python: `{**payload, "timestamp": ...}` — a key that's already
///    present keeps its EXISTING value and position; only an absent key
///    gets appended).
/// 3. Encode with [`python_dumps`]; measure the UTF-8 byte length of the
///    encoded string (Python: `len(encoded.encode("utf-8"))` — note
///    `ensure_ascii=True` means the encoded string is always pure ASCII, so
///    this equals the char count, but the byte-count framing is kept to
///    match the source's stated invariant).
/// 4. Over `max_bytes` -> reject; otherwise return the encoded string
///    (Phase 3b wires this into the actual `rpc.datastore(...)` call).
pub fn datastore_envelope(
    mut payload: PyDict,
    now: i64,
    max_bytes: usize,
) -> Result<String, TelemetryError> {
    if payload.contains_key("error") {
        return Err(TelemetryError::ErrorKeyPresent);
    }
    if !payload.contains_key("timestamp") {
        payload.push("timestamp", PyVal::Int(now));
    }
    let encoded = python_dumps(&PyVal::Dict(payload));
    // `String::len()` already counts UTF-8 bytes (not chars), matching
    // Python's `len(encoded.encode("utf-8"))` exactly.
    let bytes = encoded.len();
    if bytes > max_bytes {
        return Err(TelemetryError::TooLarge { bytes, max_bytes });
    }
    Ok(encoded)
}
