//! Python-compatible JSON writer for persisted state blobs: reproduces
//! `json.dumps` float formatting (`repr`) and separators so
//! `v2_state_json` round-trips losslessly against Python-written blobs.
//!
//! Filled in by Phase 4 Task 3 (Wave 1).
//!
//! ## Why not `serde_json::Value`
//!
//! Without the workspace `serde_json` `preserve_order` feature (off-limits —
//! shared-file ban, and it would silently change `canonical_json`'s input
//! ordering elsewhere in the workspace), `serde_json::Value::Object` is
//! backed by a `BTreeMap`: keys always come out alphabetically sorted, never
//! in the order Python wrote them. `GaussianThompsonState::to_dict()`'s key
//! order is fixed by the literal order of the Python function body (not
//! alphabetical), and unknown top-level keys must survive a load→dump cycle
//! in a reproducible position. Byte-identical parity with
//! `json.dumps(..., separators=(", ", ": "))` (the default separators —
//! NOT the compact `(",", ":")` canonical form `revops_core::canonical`
//! uses) needs an order-preserving tree, so this module implements one
//! locally: [`OValue`], a `Vec<(String, OValue)>`-backed object plus a
//! hand-rolled recursive-descent parser and a `json.dumps`-parity writer.
//!
//! This is the Decision locked in by the Phase 4 plan (Task 3 section):
//! `thompson::serde` and `state_store` (Task 9) parse blobs into `OValue`,
//! convert typed fields out of it, and keep unparsed subtrees verbatim for
//! lossless re-emission.

use revops_econ::pyfloat::py_repr;

/// An order-preserving JSON value tree. Objects are `Vec<(String, OValue)>`
/// (insertion order, not sorted) rather than a map type, and numbers keep
/// the int/float distinction Python's `json` module makes when decoding
/// (`json.loads("200")` is an `int`; `json.loads("200.0")` is a `float`) —
/// both matter for byte-identical re-emission of a Python-written blob.
#[derive(Debug, Clone, PartialEq)]
pub enum OValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Arr(Vec<OValue>),
    Obj(Vec<(String, OValue)>),
}

impl OValue {
    pub fn obj(entries: Vec<(String, OValue)>) -> Self {
        OValue::Obj(entries)
    }

    pub fn arr(items: Vec<OValue>) -> Self {
        OValue::Arr(items)
    }

    pub fn str(s: impl Into<String>) -> Self {
        OValue::Str(s.into())
    }

    pub fn as_obj(&self) -> Option<&[(String, OValue)]> {
        match self {
            OValue::Obj(entries) => Some(entries),
            _ => None,
        }
    }

    pub fn as_arr(&self) -> Option<&[OValue]> {
        match self {
            OValue::Arr(items) => Some(items),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            OValue::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Numeric coercion accepting both `Int` and `Float` (mirrors Python's
    /// untyped `d.get(key)` returning whichever the JSON decoder produced).
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            OValue::Int(i) => Some(*i as f64),
            OValue::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// `int(x)`-style coercion: truncates a float toward zero (matches
    /// Python's `int()` on a float; Python's `int()` on a non-integer
    /// *string* raises, which callers must handle themselves via `as_str`).
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            OValue::Int(i) => Some(*i),
            OValue::Float(f) if f.is_finite() => Some(*f as i64),
            OValue::Bool(b) => Some(if *b { 1 } else { 0 }),
            _ => None,
        }
    }

    pub fn get<'a>(&'a self, key: &str) -> Option<&'a OValue> {
        match self {
            OValue::Obj(entries) => entries.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, OValue::Null)
    }
}

// ---------------------------------------------------------------------------
// Parser: hand-rolled recursive descent, order-preserving objects, int/float
// distinction on numbers (mirrors CPython's `json` scanner).
// ---------------------------------------------------------------------------

pub fn parse(s: &str) -> Result<OValue, String> {
    let mut p = Parser {
        chars: s.chars().collect(),
        pos: 0,
    };
    p.skip_ws();
    let v = p.parse_value()?;
    p.skip_ws();
    if p.pos != p.chars.len() {
        return Err(format!("trailing data at char {}", p.pos));
    }
    Ok(v)
}

struct Parser {
    chars: Vec<char>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(' ' | '\t' | '\n' | '\r')) {
            self.pos += 1;
        }
    }

    fn expect(&mut self, c: char) -> Result<(), String> {
        match self.bump() {
            Some(x) if x == c => Ok(()),
            other => Err(format!(
                "expected '{c}', got {other:?} at char {}",
                self.pos
            )),
        }
    }

    fn parse_value(&mut self) -> Result<OValue, String> {
        self.skip_ws();
        match self.peek() {
            Some('{') => self.parse_object(),
            Some('[') => self.parse_array(),
            Some('"') => Ok(OValue::Str(self.parse_string()?)),
            Some('t') => {
                self.expect_lit("true")?;
                Ok(OValue::Bool(true))
            }
            Some('f') => {
                self.expect_lit("false")?;
                Ok(OValue::Bool(false))
            }
            Some('n') => {
                self.expect_lit("null")?;
                Ok(OValue::Null)
            }
            Some('N') => {
                self.expect_lit("NaN")?;
                Ok(OValue::Float(f64::NAN))
            }
            Some('I') => {
                self.expect_lit("Infinity")?;
                Ok(OValue::Float(f64::INFINITY))
            }
            Some(c) if c == '-' || c.is_ascii_digit() => self.parse_number(),
            other => Err(format!("unexpected {other:?} at char {}", self.pos)),
        }
    }

    fn expect_lit(&mut self, lit: &str) -> Result<(), String> {
        for c in lit.chars() {
            self.expect(c)?;
        }
        Ok(())
    }

    fn parse_object(&mut self) -> Result<OValue, String> {
        self.expect('{')?;
        let mut entries = Vec::new();
        self.skip_ws();
        if self.peek() == Some('}') {
            self.pos += 1;
            return Ok(OValue::Obj(entries));
        }
        loop {
            self.skip_ws();
            let key = self.parse_string()?;
            self.skip_ws();
            self.expect(':')?;
            let val = self.parse_value()?;
            entries.push((key, val));
            self.skip_ws();
            match self.bump() {
                Some(',') => continue,
                Some('}') => break,
                other => return Err(format!("expected ',' or '}}', got {other:?}")),
            }
        }
        Ok(OValue::Obj(entries))
    }

    fn parse_array(&mut self) -> Result<OValue, String> {
        self.expect('[')?;
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(']') {
            self.pos += 1;
            return Ok(OValue::Arr(items));
        }
        loop {
            let val = self.parse_value()?;
            items.push(val);
            self.skip_ws();
            match self.bump() {
                Some(',') => continue,
                Some(']') => break,
                other => return Err(format!("expected ',' or ']', got {other:?}")),
            }
        }
        Ok(OValue::Arr(items))
    }

    fn parse_string(&mut self) -> Result<String, String> {
        self.expect('"')?;
        let mut out = String::new();
        loop {
            match self.bump() {
                None => return Err("unterminated string".to_string()),
                Some('"') => break,
                Some('\\') => match self.bump() {
                    Some('"') => out.push('"'),
                    Some('\\') => out.push('\\'),
                    Some('/') => out.push('/'),
                    Some('b') => out.push('\u{08}'),
                    Some('f') => out.push('\u{0c}'),
                    Some('n') => out.push('\n'),
                    Some('r') => out.push('\r'),
                    Some('t') => out.push('\t'),
                    Some('u') => {
                        let cp = self.parse_hex4()?;
                        if (0xd800..=0xdbff).contains(&cp) {
                            // High surrogate: expect a low surrogate next.
                            self.expect('\\')?;
                            self.expect('u')?;
                            let lo = self.parse_hex4()?;
                            if !(0xdc00..=0xdfff).contains(&lo) {
                                return Err("invalid low surrogate".to_string());
                            }
                            let c = 0x10000 + ((cp - 0xd800) << 10) + (lo - 0xdc00);
                            out.push(char::from_u32(c).ok_or("invalid surrogate pair")?);
                        } else {
                            out.push(char::from_u32(cp).ok_or("invalid \\u escape")?);
                        }
                    }
                    other => return Err(format!("invalid escape {other:?}")),
                },
                Some(c) => out.push(c),
            }
        }
        Ok(out)
    }

    fn parse_hex4(&mut self) -> Result<u32, String> {
        let mut v: u32 = 0;
        for _ in 0..4 {
            let c = self.bump().ok_or("truncated \\u escape")?;
            let d = c.to_digit(16).ok_or("invalid hex digit")?;
            v = v * 16 + d;
        }
        Ok(v)
    }

    fn parse_number(&mut self) -> Result<OValue, String> {
        let start = self.pos;
        if self.peek() == Some('-') {
            self.pos += 1;
            if self.peek() == Some('I') {
                self.expect_lit("Infinity")?;
                return Ok(OValue::Float(f64::NEG_INFINITY));
            }
        }
        let mut is_float = false;
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            self.pos += 1;
        }
        if self.peek() == Some('.') {
            is_float = true;
            self.pos += 1;
            while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        if matches!(self.peek(), Some('e' | 'E')) {
            is_float = true;
            self.pos += 1;
            if matches!(self.peek(), Some('+' | '-')) {
                self.pos += 1;
            }
            while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        let text: String = self.chars[start..self.pos].iter().collect();
        if is_float {
            text.parse::<f64>()
                .map(OValue::Float)
                .map_err(|e| format!("bad float {text}: {e}"))
        } else {
            match text.parse::<i64>() {
                Ok(i) => Ok(OValue::Int(i)),
                // Integer literal too large for i64 (not expected for any
                // real fee-controller field) — fall back to float rather
                // than fail closed.
                Err(_) => text
                    .parse::<f64>()
                    .map(OValue::Float)
                    .map_err(|e| format!("bad int {text}: {e}")),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Writer: `json.dumps(v, separators=(", ", ": "))` byte parity —
// insertion-order objects, `ensure_ascii=True` string escaping, Python float
// `repr`, and the `NaN`/`Infinity`/`-Infinity` non-standard literals
// `json.dumps` emits by default (`allow_nan=True`).
// ---------------------------------------------------------------------------

pub fn dumps_python(v: &OValue) -> String {
    let mut out = String::new();
    write_value(v, &mut out);
    out
}

fn write_value(v: &OValue, out: &mut String) {
    match v {
        OValue::Null => out.push_str("null"),
        OValue::Bool(true) => out.push_str("true"),
        OValue::Bool(false) => out.push_str("false"),
        OValue::Int(i) => out.push_str(&i.to_string()),
        OValue::Float(f) => out.push_str(&py_json_float(*f)),
        OValue::Str(s) => write_escaped_string(s, out),
        OValue::Arr(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_value(item, out);
            }
            out.push(']');
        }
        OValue::Obj(entries) => {
            out.push('{');
            for (i, (k, val)) in entries.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_escaped_string(k, out);
                out.push_str(": ");
                write_value(val, out);
            }
            out.push('}');
        }
    }
}

/// `json.dumps` float formatting: CPython `repr(float)` for finite values,
/// but `NaN`/`Infinity`/`-Infinity` (no quotes) for non-finite ones — the
/// `allow_nan=True` default, distinct from `py_repr`'s lowercase
/// `nan`/`inf`/`-inf` (which is for a different call site, the shadow-cycle
/// `Explanation` float, not `json.dumps`).
fn py_json_float(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f > 0.0 {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        }
    } else {
        py_repr(f)
    }
}

/// `json.dumps(s)` with the default `ensure_ascii=True`: escapes `"`, `\`,
/// the short control-char forms (`\b \f \n \r \t`), every other char
/// outside the printable ASCII range `0x20..=0x7e` as `\u00XX`, and any
/// non-ASCII codepoint as `\uXXXX` (surrogate pair for codepoints above the
/// BMP) — verified against live CPython (`café`, DEL, control chars,
/// astral-plane emoji all escape exactly this way).
fn write_escaped_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (' '..='~').contains(&c) => out.push(c),
            c => {
                let cp = c as u32;
                if cp <= 0xffff {
                    out.push_str(&format!("\\u{cp:04x}"));
                } else {
                    let v = cp - 0x10000;
                    let hi = 0xd800 + (v >> 10);
                    let lo = 0xdc00 + (v & 0x3ff);
                    out.push_str(&format!("\\u{hi:04x}\\u{lo:04x}"));
                }
            }
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_object_preserves_order() {
        let src = r#"{"b": 1, "a": 2.5, "c": [1, 2, "x"]}"#;
        let v = parse(src).unwrap();
        assert_eq!(dumps_python(&v), r#"{"b": 1, "a": 2.5, "c": [1, 2, "x"]}"#);
    }

    #[test]
    fn int_vs_float_distinction_preserved() {
        assert_eq!(parse("200").unwrap(), OValue::Int(200));
        assert_eq!(parse("200.0").unwrap(), OValue::Float(200.0));
        assert_eq!(dumps_python(&OValue::Int(200)), "200");
        assert_eq!(dumps_python(&OValue::Float(200.0)), "200.0");
    }

    #[test]
    fn escapes_non_ascii_control_and_astral() {
        let v =
            OValue::Str("café \u{7f} \u{1f} \u{08}\u{0c} \u{1f600} \u{2603} normal".to_string());
        assert_eq!(
            dumps_python(&v),
            "\"caf\\u00e9 \\u007f \\u001f \\b\\f \\ud83d\\ude00 \\u2603 normal\""
        );
    }

    #[test]
    fn empty_containers() {
        assert_eq!(dumps_python(&OValue::Obj(vec![])), "{}");
        assert_eq!(dumps_python(&OValue::Arr(vec![])), "[]");
    }

    #[test]
    fn non_finite_floats_use_json_literals() {
        assert_eq!(dumps_python(&OValue::Float(f64::NAN)), "NaN");
        assert_eq!(dumps_python(&OValue::Float(f64::INFINITY)), "Infinity");
        assert_eq!(dumps_python(&OValue::Float(f64::NEG_INFINITY)), "-Infinity");
    }

    #[test]
    fn nested_unicode_round_trip() {
        let src = r#""😀""#;
        let v = parse(src).unwrap();
        assert_eq!(v, OValue::Str("\u{1f600}".to_string()));
        // dumps_python always escapes (ensure_ascii=True) — the astral
        // codepoint comes back out as the same surrogate-pair escape it
        // would have been parsed from, not the raw UTF-8 char.
        assert_eq!(dumps_python(&v), "\"\\ud83d\\ude00\"");

        // But parsing that literal escaped form must round-trip too.
        let escaped = parse("\"\\ud83d\\ude00\"").unwrap();
        assert_eq!(escaped, v);
    }
}
