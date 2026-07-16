//! Append-only econ ledger + replay (port of `modules/econ_ledger.py`).
//!
//! One auditable append-only event stream for proposed / rejected /
//! authorized / executed / reconciled economic actions. Events are
//! append-only: corrections are NEW events, never updates — this module
//! exposes no update/delete surface (mirrors the Python docstring's stated
//! invariant).
//!
//! `replay` reconstructs budget reservations, spend, and terminal intent
//! state (the Workstream E acceptance criterion; also the Phase 2
//! budget-truth contract for this port). See [`EconLedger::replay`] for the
//! exact rules, ported verbatim from the Python source.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::Connection;
use serde_json::Value;

use crate::types::{EconError, EconResult};

/// Ledger event vocabulary — mirrors Python's `EVENT_TYPES` tuple exactly,
/// same order, including the trailing `snapshot_created` (PR 3a: a
/// canonical snapshot was built and served to policies; recorded for the
/// audit trail but ignored by [`EconLedger::replay`]).
pub const EVENT_TYPES: [&str; 13] = [
    "intent_proposed",
    "intent_rejected",
    "intent_deferred",
    "intent_authorized",
    "budget_reserved",
    "execution_started",
    "execution_succeeded",
    "execution_failed",
    "execution_outcome_unknown",
    "cost_recorded",
    "reservation_released",
    "reconciliation_completed",
    "snapshot_created",
];

/// Terminal intent-lifecycle events. On replay the FIRST occurrence wins
/// (`entry().or_insert`) — duplicate callbacks are harmless, matching
/// Python's `_TERMINAL_EVENTS` frozenset + `terminal.setdefault`.
const TERMINAL_EVENTS: [&str; 5] = [
    "intent_rejected",
    "intent_deferred",
    "execution_succeeded",
    "execution_failed",
    "execution_outcome_unknown",
];

/// One durable ledger row, decoded from storage.
#[derive(Debug, Clone, PartialEq)]
pub struct LedgerEvent {
    pub event_id: i64,
    pub event_type: String,
    pub intent_id: String,
    pub idempotency_key: String,
    pub cycle_id: String,
    pub at: i64,
    pub amounts: BTreeMap<String, i64>,
    pub details: Value,
}

/// Replayed budget/spend/terminal-state projection. Mirrors Python's frozen
/// `LedgerState` dataclass field-for-field (Python's `anomalies` is a
/// `Tuple[str, ...]`; `Vec<String>` here is the equivalent append-order
/// sequence).
#[derive(Debug, Default, PartialEq)]
pub struct LedgerState {
    pub reserved_msat: BTreeMap<String, i64>,
    pub spent_msat: BTreeMap<String, i64>,
    pub total_spent_msat: i64,
    pub terminal: BTreeMap<String, String>,
    pub anomalies: Vec<String>,
}

/// Append-only sqlite-backed event ledger.
///
/// One connection per operation (fresh `Connection::open` on every call) —
/// mirrors the Python class's per-call `_connect()`, a deliberate fix for
/// the 2026-07-12 production incident where a single long-lived sqlite
/// connection was touched from multiple plugin threads ("SQLite objects
/// created in a thread can only be used in that same thread"). Event volume
/// is tiny, so the per-call connection cost is negligible. Every open sets
/// `busy_timeout` to 5000ms, matching Python's `sqlite3.connect(path,
/// timeout=5.0)`.
pub struct EconLedger {
    path: PathBuf,
}

const BUSY_TIMEOUT_MS: u64 = 5_000;

const CREATE_TABLE_SQL: &str = "CREATE TABLE IF NOT EXISTS econ_ledger_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    intent_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    cycle_id TEXT NOT NULL,
    at INTEGER NOT NULL,
    amounts_json TEXT NOT NULL,
    details_json TEXT NOT NULL
)";

fn connect(path: &Path) -> EconResult<Connection> {
    let conn = Connection::open(path).map_err(sql_err)?;
    conn.busy_timeout(Duration::from_millis(BUSY_TIMEOUT_MS))
        .map_err(sql_err)?;
    Ok(conn)
}

fn sql_err(e: rusqlite::Error) -> EconError {
    EconError {
        msg: format!("econ ledger sqlite error: {e}"),
    }
}

fn json_err(e: serde_json::Error) -> EconError {
    EconError {
        msg: format!("econ ledger JSON decode error: {e}"),
    }
}

impl EconLedger {
    /// Opens (creating the file if absent) the ledger sqlite database and
    /// ensures the table exists. Exact DDL parity with Python's `CREATE
    /// TABLE IF NOT EXISTS econ_ledger_events` (same columns, order, and
    /// types) — this is a byte-compatible dual-read target, not just a
    /// same-shape table.
    pub fn open(path: impl Into<PathBuf>) -> EconResult<Self> {
        let path = path.into();
        let conn = connect(&path)?;
        conn.execute_batch(CREATE_TABLE_SQL).map_err(sql_err)?;
        Ok(EconLedger { path })
    }

    /// Appends one event. Validation mirrors Python's runtime checks:
    /// unknown `event_type`, any of `intent_id`/`idempotency_key`/
    /// `cycle_id` empty, or `at < 0` all reject with [`EconError`].
    ///
    /// Python additionally rejects `bool`-typed amount values and amounts
    /// outside `[I64_MIN, U63_MAX]` — both are guards against Python's
    /// dynamic typing (`bool` is an `int` subclass at runtime; the caller
    /// could pass a `float` or an out-of-range `int`). This interface types
    /// `amounts` as `&BTreeMap<String, i64>`, so both failure modes are
    /// unrepresentable at the call boundary already: the Rust type system
    /// carries the invariant instead of a runtime guard (same philosophy as
    /// the checked types in `types.rs`).
    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &self,
        event_type: &str,
        intent_id: &str,
        idempotency_key: &str,
        cycle_id: &str,
        at: i64,
        amounts: &BTreeMap<String, i64>,
        details: &Value,
    ) -> EconResult<i64> {
        if !EVENT_TYPES.contains(&event_type) {
            return Err(EconError {
                msg: format!("unknown ledger event type: {event_type:?}"),
            });
        }
        if intent_id.is_empty() || idempotency_key.is_empty() || cycle_id.is_empty() {
            return Err(EconError {
                msg: "intent_id, idempotency_key, cycle_id required".to_string(),
            });
        }
        if at < 0 {
            return Err(EconError {
                msg: format!("at must be unix seconds: {at}"),
            });
        }

        let amounts_value = Value::Object(
            amounts
                .iter()
                .map(|(k, v)| (k.clone(), Value::from(*v)))
                .collect(),
        );
        let amounts_json = python_dumps_default(&amounts_value)?;
        let details_json = python_dumps_default(details)?;

        let conn = connect(&self.path)?;
        conn.execute(
            "INSERT INTO econ_ledger_events \
             (event_type, intent_id, idempotency_key, cycle_id, at, amounts_json, details_json) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                event_type,
                intent_id,
                idempotency_key,
                cycle_id,
                at,
                amounts_json,
                details_json,
            ],
        )
        .map_err(sql_err)?;
        Ok(conn.last_insert_rowid())
    }

    /// Durable event count, optionally filtered by `event_type`.
    pub fn count_events(&self, event_type: Option<&str>) -> EconResult<i64> {
        let conn = connect(&self.path)?;
        let count: i64 = match event_type {
            None => conn
                .query_row("SELECT COUNT(*) FROM econ_ledger_events", [], |r| r.get(0))
                .map_err(sql_err)?,
            Some(et) => conn
                .query_row(
                    "SELECT COUNT(*) FROM econ_ledger_events WHERE event_type = ?1",
                    [et],
                    |r| r.get(0),
                )
                .map_err(sql_err)?,
        };
        Ok(count)
    }

    /// Reads events with `event_id > since_id`, ordered by `event_id`.
    pub fn events(&self, since_id: i64) -> EconResult<Vec<LedgerEvent>> {
        let conn = connect(&self.path)?;
        let mut stmt = conn
            .prepare(
                "SELECT event_id, event_type, intent_id, idempotency_key, cycle_id, at, \
                 amounts_json, details_json FROM econ_ledger_events \
                 WHERE event_id > ?1 ORDER BY event_id",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map([since_id], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, i64>(5)?,
                    r.get::<_, String>(6)?,
                    r.get::<_, String>(7)?,
                ))
            })
            .map_err(sql_err)?;

        let mut out = Vec::new();
        for row in rows {
            let (
                event_id,
                event_type,
                intent_id,
                idempotency_key,
                cycle_id,
                at,
                amounts_json,
                details_json,
            ) = row.map_err(sql_err)?;
            let amounts: BTreeMap<String, i64> =
                serde_json::from_str(&amounts_json).map_err(json_err)?;
            let details: Value = serde_json::from_str(&details_json).map_err(json_err)?;
            out.push(LedgerEvent {
                event_id,
                event_type,
                intent_id,
                idempotency_key,
                cycle_id,
                at,
                amounts,
                details,
            });
        }
        Ok(out)
    }

    /// Replays the full event stream into a [`LedgerState`] projection.
    /// Rules ported verbatim from `EconLedger.replay` (the budget-truth
    /// contract):
    ///
    /// - `budget_reserved` **sets** `reserved_msat[key]` (duplicates are
    ///   idempotent re-announces of the same worst-case cost).
    /// - `cost_recorded` adds to `spent_msat[key]` and decrements
    ///   `reserved_msat[key]`, floored at 0. If the reservation is already
    ///   `<= 0` AND this is the first cost seen for `key`, push the anomaly
    ///   `"cost_recorded without reservation: {key}"` — spend is never free,
    ///   and replay must never crash on it.
    /// - `reservation_released` zeroes `reserved_msat[key]`.
    /// - `reconciliation_completed`: **sets** `reserved_msat[key]`
    ///   absolutely if `amounts.reserved_msat` is present (ledger corrected
    ///   to DB truth); adds `amounts.cost_msat` to spend if present; marks
    ///   `key` terminal (first-wins) only when `details.terminal` is
    ///   truthy.
    /// - The five [`TERMINAL_EVENTS`] use first-wins (`entry().or_insert`).
    /// - `snapshot_created` is ignored.
    ///
    /// Output: `reserved_msat` drops zero-valued entries (Python's `if v`
    /// truthiness test, which for `int` is exactly "nonzero").
    pub fn replay(&self) -> EconResult<LedgerState> {
        let mut reserved: BTreeMap<String, i64> = BTreeMap::new();
        let mut spent: BTreeMap<String, i64> = BTreeMap::new();
        let mut terminal: BTreeMap<String, String> = BTreeMap::new();
        let mut anomalies: Vec<String> = Vec::new();

        for event in self.events(0)? {
            let key = &event.idempotency_key;
            match event.event_type.as_str() {
                "budget_reserved" => {
                    let v = *event.amounts.get("reserved_msat").unwrap_or(&0);
                    reserved.insert(key.clone(), v);
                }
                "cost_recorded" => {
                    let cost = *event.amounts.get("cost_msat").unwrap_or(&0);
                    let cur_reserved = *reserved.get(key).unwrap_or(&0);
                    if cur_reserved <= 0 && !spent.contains_key(key) {
                        anomalies.push(format!("cost_recorded without reservation: {key}"));
                    }
                    let cur_spent = *spent.get(key).unwrap_or(&0);
                    spent.insert(key.clone(), cur_spent + cost);
                    reserved.insert(key.clone(), (cur_reserved - cost).max(0));
                }
                "reservation_released" => {
                    reserved.insert(key.clone(), 0);
                }
                "reconciliation_completed" => {
                    if let Some(v) = event.amounts.get("reserved_msat") {
                        reserved.insert(key.clone(), *v);
                    }
                    if let Some(v) = event.amounts.get("cost_msat") {
                        let cur_spent = *spent.get(key).unwrap_or(&0);
                        spent.insert(key.clone(), cur_spent + v);
                    }
                    let is_terminal = event
                        .details
                        .get("terminal")
                        .map(is_truthy)
                        .unwrap_or(false);
                    if is_terminal {
                        terminal
                            .entry(key.clone())
                            .or_insert_with(|| event.event_type.clone());
                    }
                }
                et if TERMINAL_EVENTS.contains(&et) => {
                    terminal
                        .entry(key.clone())
                        .or_insert_with(|| et.to_string());
                }
                _ => {}
            }
        }

        let total_spent_msat: i64 = spent.values().sum();
        let reserved_msat: BTreeMap<String, i64> =
            reserved.into_iter().filter(|(_, v)| *v != 0).collect();

        Ok(LedgerState {
            reserved_msat,
            spent_msat: spent,
            total_spent_msat,
            terminal,
            anomalies,
        })
    }
}

/// Python's `bool(x)` truthiness for a decoded JSON value: `null`/`false`/
/// `0`/`0.0`/`""`/`[]`/`{}` are falsy, everything else truthy. Used only for
/// `details.get("terminal")` in [`EconLedger::replay`], mirroring Python's
/// `if (event["details"] or {}).get("terminal")`.
fn is_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// Python's `json.dumps(x, sort_keys=True)` with **default** separators —
/// `(", ", ": ")` — NOT the canonical compact form (`(",", ":")`) used by
/// `revops_core::canonical::canonical_json`. This also mirrors Python's
/// default `ensure_ascii=True`: any codepoint outside printable ASCII
/// (`< 0x20` or `>= 0x7F`) is escaped as `\uXXXX`, using UTF-16 surrogate
/// pairs for codepoints above the BMP (there is no `\Uxxxxxxxx` form in
/// JSON; Python's encoder emits the same pairs).
///
/// This is the byte-compatibility contract for `amounts_json`/
/// `details_json`: a Rust-written row and a Python-written row in the same
/// `econ_ledger.db` must be byte-identical (dual-read during the Phase 2
/// shadow window). Golden strings pinned in the unit tests below were
/// generated with, from the `cl_revenue_ops-port` worktree, 2026-07-16:
///
/// ```text
/// python3 -c "
/// import json
/// samples = [
///     {},
///     {'reserved_msat': 400000},
///     {'b': 1, 'a': 2, 'c': {'z': 1, 'y': 2}},
///     {'reason': 'released'},
///     {'observed_at': 1783962518},
///     {'kind': 'ledger_stale_reservation', 'terminal': True},
///     {'empty_list': [], 'neg': -5, 'nested': [1, 2, {'x': 'y'}]},
///     {'a': 'héllo', 'b': '日本語', 'c': '😀'},
/// ]
/// for s in samples:
///     print(repr(json.dumps(s, sort_keys=True)))
/// "
/// ```
///
/// Numbers: this ledger's `amounts`/`details` values are checked integers
/// only (float input is rejected with `EconError`, mirroring the
/// integer-only invariant already enforced in `types.rs` and
/// `canonical.rs` — Python's float `repr` is deliberately not reproduced
/// here, since it diverges from Rust's `f64` formatting for values like
/// `1e-5` and this function's output is a stored, replayed row, not a
/// transient value).
pub(crate) fn python_dumps_default(v: &Value) -> EconResult<String> {
    let mut out = String::new();
    write_value(v, &mut out)?;
    Ok(out)
}

fn write_value(v: &Value, out: &mut String) -> EconResult<()> {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(n) => {
            if n.is_f64() {
                return Err(EconError {
                    msg: format!("ledger JSON forbids non-integer numbers: {n}"),
                });
            }
            out.push_str(&n.to_string());
        }
        Value::String(s) => write_string(s, out),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_value(item, out)?;
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_string(k, out);
                out.push_str(": ");
                write_value(&map[*k], out)?;
            }
            out.push('}');
        }
    }
    Ok(())
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
            c if (c as u32) < 0x20 || (c as u32) >= 0x7F => write_unicode_escape(c, out),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Emits `\uXXXX` (or a UTF-16 surrogate pair for astral codepoints) for one
/// character, matching Python's `ensure_ascii=True` encoder.
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Golden strings generated with:
    /// `python3 -c "import json; print(repr(json.dumps(<value>, sort_keys=True)))"`
    /// against the `cl_revenue_ops-port` worktree, 2026-07-16. See the
    /// [`python_dumps_default`] doc comment for the full command.
    #[test]
    fn python_dumps_default_matches_python_golden_strings() {
        let cases: Vec<(Value, &str)> = vec![
            (json!({}), "{}"),
            (
                json!({"reserved_msat": 400000}),
                r#"{"reserved_msat": 400000}"#,
            ),
            (
                json!({"b": 1, "a": 2, "c": {"z": 1, "y": 2}}),
                r#"{"a": 2, "b": 1, "c": {"y": 2, "z": 1}}"#,
            ),
            (json!({"reason": "released"}), r#"{"reason": "released"}"#),
            (
                json!({"observed_at": 1783962518}),
                r#"{"observed_at": 1783962518}"#,
            ),
            (
                json!({"kind": "ledger_stale_reservation", "terminal": true}),
                r#"{"kind": "ledger_stale_reservation", "terminal": true}"#,
            ),
            (
                json!({"empty_list": [], "neg": -5, "nested": [1, 2, {"x": "y"}]}),
                r#"{"empty_list": [], "neg": -5, "nested": [1, 2, {"x": "y"}]}"#,
            ),
        ];
        for (value, expected) in cases {
            assert_eq!(python_dumps_default(&value).unwrap(), expected);
        }
    }

    /// `ensure_ascii=True` trap: Python escapes every non-ASCII codepoint as
    /// `\uXXXX`, with UTF-16 surrogate pairs above the BMP (emoji). Golden
    /// string:
    /// `json.dumps({'a': 'héllo', 'b': '日本語', 'c': '😀'}, sort_keys=True)`
    /// == `'{"a": "h\\u00e9llo", "b": "\\u65e5\\u672c\\u8a9e", "c": "\\ud83d\\ude00"}'`
    #[test]
    fn python_dumps_default_escapes_non_ascii_like_python() {
        let value = json!({"a": "héllo", "b": "日本語", "c": "😀"});
        let expected =
            "{\"a\": \"h\\u00e9llo\", \"b\": \"\\u65e5\\u672c\\u8a9e\", \"c\": \"\\ud83d\\ude00\"}";
        assert_eq!(python_dumps_default(&value).unwrap(), expected);
    }

    /// Control chars and DEL (0x7f) are also escaped under `ensure_ascii`
    /// (Python: `json.dumps({'a': chr(0x7f)})` == `'{"a": "\\u007f"}'`;
    /// `json.dumps({'a': chr(0x1f)})` == `'{"a": "\\u001f"}'`; `'/'` is
    /// passed through unescaped).
    #[test]
    fn python_dumps_default_escapes_control_chars_and_del() {
        let value = json!({"a": "\u{7f}", "b": "\u{1f}", "c": "/"});
        let expected = "{\"a\": \"\\u007f\", \"b\": \"\\u001f\", \"c\": \"/\"}";
        assert_eq!(python_dumps_default(&value).unwrap(), expected);
    }

    #[test]
    fn python_dumps_default_rejects_float() {
        let value = json!({"a": 1.5});
        assert!(python_dumps_default(&value).is_err());
    }
}
