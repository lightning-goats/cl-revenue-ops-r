//! Assembly for `revenue-set-fee` — the manual fee write, and the
//! riskiest RPC in this port.
//!
//! Port of `revenue_set_fee` + `_revenue_set_fee_authorized`
//! (cl-revenue-ops.py:4718-4820), arm-for-arm IN PYTHON'S ORDER:
//!
//! 1. the shared fee-authority execution lease — denial answers
//!    `{"error": "Fee authority disabled", **_blocked_reason(...)}`
//!    before anything else runs;
//! 2. the usage arm (either param absent/null);
//! 3. `fee_controller is None or config is None` -> "Plugin not fully
//!    initialized" — in this port that is [`SetFeeSources::setter`]
//!    `= None`, the SEALED manual-fee execution seam;
//! 4. MAJOR-09: the force rate limit (the SHARED `ForceRateLimiter`
//!    global, keyed per command);
//! 5. `int(fee_ppm)` validation and the non-negative check;
//! 6. the channel_id type + format gates (scid / 64-hex channel id /
//!    66-hex node id);
//! 7. the non-force `[min_fee_ppm, max_fee_ppm]` range gate;
//! 8. DD2 / P1-001/P1-004: the HARD rails bind even under force —
//!    `[min_fee_ppm, min(max_fee_ppm, ABS_MAX_FEE_PPM)]` clamps the
//!    request, with the clamp disclosed in the success response;
//! 9. the controller dispatch (`manual=True`,
//!    `enforce_limits=(not force)`), success/failure dicts merged
//!    Python-style (`**result`).
//!
//! RUNTIME SAFETY: pre-Task-69 the runtime can never reach arm 9. The
//! startup fee-authority gate is disabled in every non-live mode, so arm
//! 1 denies; and even if it did not, `main.rs` passes `setter: None`
//! (there is NO manual fee execution capability until Task 69's
//! authority-gated assembly), so arm 3 answers Python's exact
//! "Plugin not fully initialized". The full pipeline below is driven by
//! tests with an injected setter.

use serde_json::{json, Value};

use crate::rpc_fee_authority_status::FeeAuthorityStatusSnapshot;
use crate::rpc_params::python_int;
use crate::rpc_rebalance_ops::ForceRateLimiter;
use revops_fees::execution::ABS_MAX_FEE_PPM;

/// py `re.match(r'^\d+[x:]\d+[x:]\d+$', ...)` — same manual scanner as
/// `rpc_analyze::matches_scid_format` (no `regex` crate in this
/// workspace); the char class admits mixed separators exactly like the
/// regex does.
fn matches_scid_format(s: &str) -> bool {
    let mut parts = s.split(['x', ':']);
    let (Some(a), Some(b), Some(c)) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    if parts.next().is_some() {
        return false;
    }
    let is_digits = |p: &str| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit());
    is_digits(a) && is_digits(b) && is_digits(c)
}

/// py `re.match(r'^[0-9a-fA-F]{64}$'...)` / the 66-hex node-id form.
fn is_hex_of_len(s: &str, len: usize) -> bool {
    s.len() == len && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// The manual fee execution seam (py `fee_controller.set_channel_fee`,
/// called with `manual=True`). Returns Python's result dict verbatim
/// (`success`, optional `message`/`fee_ppm`/`channel_id`, extras). `None`
/// at the [`SetFeeSources`] level = the capability is UNASSEMBLED
/// (pre-Task-69), Python's `fee_controller is None` arm.
pub type ManualFeeSetter<'a> = &'a dyn Fn(&str, i64, bool) -> Value;

/// Everything the assembled response needs, prefetched by `main.rs`.
pub struct SetFeeSources<'a> {
    pub gate: &'a FeeAuthorityStatusSnapshot,
    pub channel_id: Option<&'a Value>,
    pub fee_ppm: Option<&'a Value>,
    pub force: bool,
    /// `None` = no manual fee execution capability (pre-Task-69).
    pub setter: Option<ManualFeeSetter<'a>>,
    /// The SHARED force limiter (py's one global, keyed per command) —
    /// consulted ONLY on `force=true` calls, because a check records the
    /// call (py check_rate_limit appends the timestamp).
    pub rate_limiter: &'a ForceRateLimiter,
    pub min_fee_ppm: i64,
    pub max_fee_ppm: i64,
    /// Wall clock in float seconds (py `time.time()` inside the limiter).
    pub now_seconds: f64,
}

/// py treats an explicit JSON null exactly like an absent param (pyln
/// binds both to `None`).
fn present(v: Option<&Value>) -> Option<&Value> {
    v.filter(|v| !v.is_null())
}

/// Port of the full handler. See the module doc for the arm map.
pub fn set_fee_response(s: SetFeeSources<'_>) -> Value {
    if let Some(denial) = s.gate.execution_lease_denial("revenue-set-fee") {
        return denial;
    }
    let (Some(channel_id_raw), Some(fee_ppm_raw)) = (present(s.channel_id), present(s.fee_ppm))
    else {
        return json!({"error": "usage: revenue-set-fee channel_id fee_ppm [force=false]"});
    };
    let Some(setter) = s.setter else {
        return json!({"error": "Plugin not fully initialized"});
    };

    if s.force {
        if let Err(msg) = s
            .rate_limiter
            .check_rate_limit("revenue-set-fee", s.now_seconds)
        {
            return json!({"status": "error", "error": msg});
        }
    }

    // py: `int(fee_ppm)` (ValueError/TypeError -> the integer error),
    // then the non-negative check on the converted value.
    let fee_ppm = match python_int(fee_ppm_raw) {
        Ok(fee_ppm) => fee_ppm,
        Err(_) => return json!({"status": "error", "error": "fee_ppm must be an integer"}),
    };
    if fee_ppm < 0 {
        return json!({"status": "error", "error": "fee_ppm must be non-negative"});
    }

    // P1-012: type gate before the format checks.
    let Some(channel_id) = channel_id_raw.as_str() else {
        return json!({"status": "error", "error": "channel_id must be a string"});
    };
    if !(matches_scid_format(channel_id)
        || is_hex_of_len(channel_id, 64)
        || is_hex_of_len(channel_id, 66))
    {
        return json!({"status": "error", "error": "Invalid channel_id or node_id format"});
    }

    if !s.force && (fee_ppm < s.min_fee_ppm || fee_ppm > s.max_fee_ppm) {
        return json!({
            "status": "error",
            "error": format!(
                "Fee {fee_ppm} is outside configured range [{}, {}]. Use force=true to override.",
                s.min_fee_ppm, s.max_fee_ppm
            ),
        });
    }

    // DD2 / P1-001, P1-004: the hard rails bind even under force.
    let hard_min = s.min_fee_ppm;
    let hard_max = s.max_fee_ppm.min(ABS_MAX_FEE_PPM);
    let requested_fee_ppm = fee_ppm;
    // py `max(hard_min, min(hard_max, fee_ppm))` (cl-revenue-ops.py:4794)
    // -- TOTAL, even when an out-of-range config override inverts the
    // rail. `Ord::clamp` PANICS when min > max, which would take the
    // whole plugin down on a mis-set config (self-review 2026-07-31).
    let fee_ppm = hard_min.max(hard_max.min(fee_ppm));
    let fee_rail_clamped = fee_ppm != requested_fee_ppm;

    // py: `enforce_limits=(not force)`; exceptions from the controller
    // become the generic error arm — the setter seam returns dicts, so
    // that arm lives inside the seam's own implementation.
    let result = setter(channel_id, fee_ppm, !s.force);
    let success = result.get("success").map(is_truthy).unwrap_or(false);
    if !success {
        let message = result
            .get("message")
            .and_then(Value::as_str)
            .filter(|m| !m.is_empty())
            .unwrap_or("Fee update failed");
        let mut out = json!({"status": "error", "error": message});
        merge(&mut out, &result);
        return out;
    }
    let applied_fee = result.get("fee_ppm").cloned().unwrap_or(json!(fee_ppm));
    let resolved_channel = result
        .get("channel_id")
        .cloned()
        .unwrap_or_else(|| json!(channel_id));
    let mut out = json!({
        "status": "success",
        "channel": resolved_channel,
        "new_fee_ppm": applied_fee,
    });
    merge(&mut out, &result);
    if fee_rail_clamped {
        out["requested_fee_ppm"] = json!(requested_fee_ppm);
        out["clamped_to_rail"] = json!([hard_min, hard_max]);
    }
    out
}

/// py `{**base, **result}`: result's keys win on collision.
fn merge(out: &mut Value, result: &Value) {
    if let (Some(out_map), Some(result_map)) = (out.as_object_mut(), result.as_object()) {
        for (key, value) in result_map {
            out_map.insert(key.clone(), value.clone());
        }
    }
}

/// py truthiness of `result.get("success")` — dict `.get` may hand back
/// any JSON shape.
fn is_truthy(v: &Value) -> bool {
    crate::rpc_params::is_truthy_py(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scid_and_hex_formats_match_pythons_regexes() {
        assert!(matches_scid_format("123x456x0"));
        assert!(matches_scid_format("123:456:0"));
        assert!(
            matches_scid_format("1x2:3"),
            "regex char class admits mixed separators"
        );
        assert!(!matches_scid_format("12x34"));
        assert!(!matches_scid_format("1x2x3x4"));
        assert!(!matches_scid_format("x2x3"));
        let hex64 = "a".repeat(64);
        let hex66 = "B".repeat(66);
        let hex65 = "a".repeat(65);
        assert!(is_hex_of_len(&hex64, 64));
        assert!(is_hex_of_len(&hex66, 66));
        assert!(!is_hex_of_len(&hex65, 64));
        assert!(!is_hex_of_len("zz", 64));
    }
}
