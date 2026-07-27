//! Pure parsing/normalization helpers for boltzcli JSON payloads.
//!
//! Ports py `BoltzCliManager._parse_int`/`_parse_timestamp`/
//! `_normalize_swap_entry`/`_extract_swap_list`/`_contains_chanids_cln_error`/
//! `_primary_swap_entry` (boltz_manager.py:510-543, 872-924, 1205-1222).
//! boltzcli's msat/int-like fields may arrive as a JSON number, a numeric
//! string, or (for the wrapper shapes) nested one level deep — these
//! helpers absorb that the same way Python's duck-typed parsing does.

use serde_json::{Map, Value};

/// py `_parse_int` (boltz_manager.py:510-530): never errors, falls back to
/// `default`. Order of attempts matches Python exactly: `None` -> default,
/// bool -> `int(bool)` (0/1), int/float -> truncate, else parse the
/// stringified value as an int, then (fallback) as a float truncated to
/// int; anything else -> default.
pub fn parse_int(v: &Value, default: i64) -> i64 {
    match v {
        Value::Null => default,
        Value::Bool(b) => *b as i64,
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i
            } else if let Some(f) = n.as_f64() {
                f.trunc() as i64
            } else {
                default
            }
        }
        Value::String(s) => {
            let t = s.trim();
            if t.is_empty() {
                return default;
            }
            if let Ok(i) = t.parse::<i64>() {
                return i;
            }
            match t.parse::<f64>() {
                Ok(f) if f.is_finite() => f.trunc() as i64,
                _ => default,
            }
        }
        _ => default,
    }
}

/// py `_parse_timestamp` (boltz_manager.py:532-543): `parse_int` with a 0
/// default, then reject non-positive and pre-2001-01-01 sanity-check
/// failures (boltz sometimes emits `-62135596800`-style default
/// timestamps) by returning `None`.
pub fn parse_timestamp(v: &Value) -> Option<i64> {
    let ts = parse_int(v, 0);
    if ts <= 0 {
        return None;
    }
    if ts < 978_307_200 {
        return None;
    }
    Some(ts)
}

/// py `_normalize_swap_entry` (boltz_manager.py:872-889): flatten nested
/// swap entries from boltzcli `listswaps`/`swapinfo` outputs. Nested
/// `swap`/`reverseSwap`/`chainSwap`/`channelCreation` fields (checked in
/// that fixed order) take precedence over sibling fields when present with
/// an `id`; the wrapper key is otherwise stamped as `_swap_wrapper`.
pub fn normalize_swap_entry(entry: &Value, wrapper_key: Option<&str>) -> Map<String, Value> {
    let obj = match entry.as_object() {
        Some(o) => o.clone(),
        None => return Map::new(),
    };
    let mut out = obj;
    if let Some(wk) = wrapper_key {
        if !out.contains_key(wk) {
            out.insert("_swap_wrapper".to_string(), Value::String(wk.to_string()));
        }
    }
    for k in ["swap", "reverseSwap", "chainSwap", "channelCreation"] {
        if let Some(nested) = out.get(k).and_then(|v| v.as_object()) {
            if nested.get("id").is_some() {
                let mut merged = out.clone();
                merged.remove(k);
                for (nk, nv) in nested {
                    merged.insert(nk.clone(), nv.clone());
                }
                merged.insert("_swap_wrapper".to_string(), Value::String(k.to_string()));
                return merged;
            }
        }
    }
    out
}

/// py `_extract_swap_list` (boltz_manager.py:891-924): pull a flat,
/// id-deduplicated (first-seen order preserved) list of swap entries out
/// of any of boltzcli's several `listswaps`/`swapinfo` JSON shapes.
pub fn extract_swap_list(swaps_json: &Value) -> Vec<Map<String, Value>> {
    let mut items: Vec<Map<String, Value>> = Vec::new();
    match swaps_json {
        Value::Object(obj) => {
            for key in ["swaps", "list", "allSwaps"] {
                if let Some(Value::Array(arr)) = obj.get(key) {
                    for s in arr {
                        if s.is_object() {
                            items.push(normalize_swap_entry(s, None));
                        }
                    }
                }
            }
            for key in [
                "reverseSwaps",
                "submarineSwaps",
                "chainSwaps",
                "channelCreations",
            ] {
                if let Some(Value::Array(arr)) = obj.get(key) {
                    for s in arr {
                        if s.is_object() {
                            items.push(normalize_swap_entry(s, Some(key)));
                        }
                    }
                }
            }
            for key in ["swap", "reverseSwap", "chainSwap", "channelCreation"] {
                if let Some(val) = obj.get(key) {
                    if let Some(vo) = val.as_object() {
                        if vo.get("id").is_some() {
                            let mut wrapper = Map::new();
                            wrapper.insert(key.to_string(), val.clone());
                            items.push(normalize_swap_entry(&Value::Object(wrapper), Some(key)));
                        }
                    }
                }
            }
        }
        Value::Array(arr) => {
            for s in arr {
                if s.is_object() {
                    items.push(normalize_swap_entry(s, None));
                }
            }
        }
        _ => {}
    }

    let mut dedup: Vec<Map<String, Value>> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for it in items {
        if it.is_empty() {
            continue;
        }
        let sid = it.get("id").map(value_to_id_string).unwrap_or_default();
        let key = if !sid.is_empty() {
            sid
        } else {
            // Best-effort stable fallback key; Python uses
            // `json.dumps(it, sort_keys=True, default=str)`. serde_json's
            // Map is a BTreeMap by default in this workspace (see the
            // `preserve_order` feature is NOT enabled), so `to_string`
            // already emits keys in sorted order — an equivalent
            // dedup key for entries with no id.
            Value::Object(it.clone()).to_string()
        };
        if !seen.insert(key) {
            continue;
        }
        dedup.push(it);
    }
    dedup
}

fn value_to_id_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// py `_contains_chanids_cln_error` (boltz_manager.py:1205-1213): detect
/// the exact `"chanIds are not supported for cln"` error contract
/// (case-insensitive substring), checked both at the payload's top-level
/// `error` field and inside every extracted swap entry's own `error`
/// field. This drives the reverse-swap capability cache
/// (`_reverse_chanids_supported`) — the string match is load-bearing and
/// must not be "cleaned up" to a different needle.
pub fn contains_chanids_cln_error(payload: &Value) -> bool {
    const NEEDLE: &str = "chanids are not supported for cln";
    if let Some(obj) = payload.as_object() {
        if let Some(Value::String(e)) = obj.get("error") {
            if e.to_lowercase().contains(NEEDLE) {
                return true;
            }
        }
        for s in extract_swap_list(payload) {
            if let Some(Value::String(e)) = s.get("error") {
                if e.to_lowercase().contains(NEEDLE) {
                    return true;
                }
            }
        }
    }
    false
}

/// py `_primary_swap_entry` (boltz_manager.py:1215-1222).
pub fn primary_swap_entry(payload: &Value) -> Option<Map<String, Value>> {
    let obj = payload.as_object()?;
    let extracted = extract_swap_list(payload);
    if let Some(first) = extracted.into_iter().next() {
        return Some(first);
    }
    if obj.get("id").is_some() {
        return Some(normalize_swap_entry(payload, None));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_int_null_uses_default() {
        assert_eq!(parse_int(&Value::Null, 7), 7);
    }

    #[test]
    fn parse_int_bool_is_zero_or_one() {
        // Control: bool is NOT passed through as if it were an int equal to
        // the default; True/False map to 1/0 specifically.
        assert_eq!(parse_int(&json!(true), 99), 1);
        assert_eq!(parse_int(&json!(false), 99), 0);
    }

    #[test]
    fn parse_int_float_truncates_toward_zero() {
        assert_eq!(parse_int(&json!(3.9), 0), 3);
        assert_eq!(parse_int(&json!(-3.9), 0), -3);
    }

    #[test]
    fn parse_int_numeric_string() {
        assert_eq!(parse_int(&json!("42"), 0), 42);
        assert_eq!(parse_int(&json!("  42  "), 0), 42);
    }

    #[test]
    fn parse_int_garbage_string_uses_default() {
        assert_eq!(parse_int(&json!("not a number"), -1), -1);
    }

    #[test]
    fn parse_timestamp_rejects_pre_2001_sentinel() {
        // -62135596800 is a known boltzd zero-timestamp sentinel.
        assert_eq!(parse_timestamp(&json!(-62135596800i64)), None);
    }

    #[test]
    fn parse_timestamp_accepts_recent() {
        assert_eq!(parse_timestamp(&json!(1_700_000_000)), Some(1_700_000_000));
    }

    #[test]
    fn parse_timestamp_zero_is_none() {
        assert_eq!(parse_timestamp(&json!(0)), None);
    }

    #[test]
    fn normalize_flattens_nested_reverse_swap() {
        let entry = json!({"reverseSwap": {"id": "abc", "state": "pending"}, "extra": 1});
        let out = normalize_swap_entry(&entry, None);
        assert_eq!(out.get("id").unwrap(), "abc");
        assert_eq!(out.get("_swap_wrapper").unwrap(), "reverseSwap");
        assert_eq!(out.get("extra").unwrap(), 1);
    }

    #[test]
    fn normalize_without_nested_id_keeps_flat() {
        // Control: a nested dict with no "id" must NOT trigger the merge.
        let entry = json!({"swap": {"note": "no id here"}, "id": "top"});
        let out = normalize_swap_entry(&entry, None);
        assert_eq!(out.get("id").unwrap(), "top");
        assert!(out.get("_swap_wrapper").is_none());
    }

    #[test]
    fn extract_swap_list_dedupes_by_id() {
        let payload = json!({
            "swaps": [{"id": "a"}, {"id": "a"}, {"id": "b"}],
        });
        let list = extract_swap_list(&payload);
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn extract_swap_list_splits_by_type_key() {
        let payload = json!({
            "reverseSwaps": [{"id": "r1"}],
            "submarineSwaps": [{"id": "s1"}],
        });
        let list = extract_swap_list(&payload);
        let ids: Vec<String> = list
            .iter()
            .map(|m| m.get("id").unwrap().as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, vec!["r1".to_string(), "s1".to_string()]);
    }

    #[test]
    fn chanids_error_detected_case_insensitive_top_level() {
        let payload = json!({"error": "ChanIds Are Not Supported For CLN backend"});
        assert!(contains_chanids_cln_error(&payload));
    }

    #[test]
    fn chanids_error_detected_in_nested_swap_entry() {
        let payload = json!({"swaps": [{"id": "x", "error": "chanIds are not supported for cln"}]});
        assert!(contains_chanids_cln_error(&payload));
    }

    #[test]
    fn chanids_error_absent_for_unrelated_error() {
        // Control: an unrelated error string must not trip the needle.
        let payload = json!({"error": "insufficient funds"});
        assert!(!contains_chanids_cln_error(&payload));
    }

    #[test]
    fn primary_swap_entry_prefers_extracted_list() {
        let payload = json!({"swaps": [{"id": "first"}, {"id": "second"}]});
        let primary = primary_swap_entry(&payload).unwrap();
        assert_eq!(primary.get("id").unwrap(), "first");
    }

    #[test]
    fn primary_swap_entry_falls_back_to_flat_id() {
        let payload = json!({"id": "solo", "state": "pending"});
        let primary = primary_swap_entry(&payload).unwrap();
        assert_eq!(primary.get("id").unwrap(), "solo");
    }

    #[test]
    fn primary_swap_entry_none_without_id_or_list() {
        let payload = json!({"note": "nothing here"});
        assert!(primary_swap_entry(&payload).is_none());
    }
}
