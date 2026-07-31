//! Integration tests for `revenue-set-fee`'s production assembly
//! (`rpc_set_fee::set_fee_response`, the exact function `main.rs`
//! registers), arm-by-arm against cl-revenue-ops.py:4718-4820. The
//! controller seam is injected (the real one is UNASSEMBLED until Task
//! 69); the authority gate and force limiter are the real production
//! types.

use std::cell::RefCell;

use revops::rpc_fee_authority_status::FeeAuthorityStatusSnapshot;
use revops::rpc_rebalance_ops::ForceRateLimiter;
use revops::rpc_set_fee::{set_fee_response, SetFeeSources};
use serde_json::{json, Value};

const NOW: f64 = 1_800_000_000.0;

fn enabled_gate() -> FeeAuthorityStatusSnapshot {
    FeeAuthorityStatusSnapshot::from_startup_mode(true, 1_700_000_000)
}

fn sources<'a>(
    gate: &'a FeeAuthorityStatusSnapshot,
    channel_id: Option<&'a Value>,
    fee_ppm: Option<&'a Value>,
    force: bool,
    setter: Option<revops::rpc_set_fee::ManualFeeSetter<'a>>,
    rate_limiter: &'a ForceRateLimiter,
) -> SetFeeSources<'a> {
    SetFeeSources {
        gate,
        channel_id,
        fee_ppm,
        force,
        setter,
        rate_limiter,
        min_fee_ppm: 10,
        max_fee_ppm: 3000,
        now_seconds: NOW,
    }
}

/// py 4721-4728: the lease denial dict, byte-for-byte — and it fires
/// BEFORE the usage arm (no params here) and before any setter concern.
#[test]
fn disabled_gate_denies_before_everything_else() {
    let gate = FeeAuthorityStatusSnapshot::from_startup_mode(false, 1_700_000_123);
    let limiter = ForceRateLimiter::production();
    let v = set_fee_response(sources(&gate, None, None, true, None, &limiter));
    assert_eq!(
        v,
        json!({
            "error": "Fee authority disabled",
            "status": "blocked",
            "reason": "fee_authority_disabled",
            "operation": "revenue-set-fee",
            "generation": 1,
            "transitioned_at": 1_700_000_123,
        })
    );
}

/// py 4744-4745: either param absent (or explicit null, which pyln binds
/// to None) is the usage string.
#[test]
fn missing_or_null_params_are_the_usage_arm() {
    let gate = enabled_gate();
    let limiter = ForceRateLimiter::production();
    let usage = json!({"error": "usage: revenue-set-fee channel_id fee_ppm [force=false]"});
    let channel = json!("100x1x0");
    let fee = json!(100);
    let null = Value::Null;
    assert_eq!(
        set_fee_response(sources(&gate, Some(&channel), None, false, None, &limiter)),
        usage
    );
    assert_eq!(
        set_fee_response(sources(&gate, None, Some(&fee), false, None, &limiter)),
        usage
    );
    assert_eq!(
        set_fee_response(sources(
            &gate,
            Some(&channel),
            Some(&null),
            false,
            None,
            &limiter
        )),
        usage
    );
}

/// py 4746-4747: `fee_controller is None` -> "Plugin not fully
/// initialized" — the pre-Task-69 runtime arm (setter unassembled).
#[test]
fn unassembled_setter_is_plugin_not_fully_initialized() {
    let gate = enabled_gate();
    let limiter = ForceRateLimiter::production();
    let channel = json!("100x1x0");
    let fee = json!(100);
    assert_eq!(
        set_fee_response(sources(
            &gate,
            Some(&channel),
            Some(&fee),
            false,
            None,
            &limiter
        )),
        json!({"error": "Plugin not fully initialized"})
    );
}

/// MAJOR-09 (py 4750-4753): the 11th force call inside the window is
/// refused with the limiter's verbatim message; non-force calls never
/// consult (or consume) the limiter.
#[test]
fn force_rate_limit_binds_only_force_calls() {
    let gate = enabled_gate();
    let limiter = ForceRateLimiter::new(1, 60.0);
    let channel = json!("100x1x0");
    let fee = json!(100);
    let setter = |_: &str, fee_ppm: i64, _: bool| json!({"success": true, "fee_ppm": fee_ppm});

    // Non-force calls do not consume the budget.
    for _ in 0..3 {
        let v = set_fee_response(sources(
            &gate,
            Some(&channel),
            Some(&fee),
            false,
            Some(&setter),
            &limiter,
        ));
        assert_eq!(v["status"], "success");
    }
    // First force call consumes the single slot...
    let v = set_fee_response(sources(
        &gate,
        Some(&channel),
        Some(&fee),
        true,
        Some(&setter),
        &limiter,
    ));
    assert_eq!(v["status"], "success");
    // ...second is refused with the Python message shape.
    let v = set_fee_response(sources(
        &gate,
        Some(&channel),
        Some(&fee),
        true,
        Some(&setter),
        &limiter,
    ));
    assert_eq!(v["status"], "error");
    assert!(
        v["error"]
            .as_str()
            .unwrap()
            .starts_with("Rate limit exceeded for force=revenue-set-fee."),
        "{v:?}"
    );
}

/// py 4756-4761: int() semantics — "junk" is the integer error, -5 the
/// non-negative error, bool True is int 1 (a Python int subclass), and
/// 1 ppm is then below min_fee_ppm -> the range gate.
#[test]
fn fee_ppm_validation_matches_python_int() {
    let gate = enabled_gate();
    let limiter = ForceRateLimiter::production();
    let channel = json!("100x1x0");
    let setter = |_: &str, fee_ppm: i64, _: bool| json!({"success": true, "fee_ppm": fee_ppm});

    let junk = json!("junk");
    let v = set_fee_response(sources(
        &gate,
        Some(&channel),
        Some(&junk),
        false,
        Some(&setter),
        &limiter,
    ));
    assert_eq!(
        v,
        json!({"status": "error", "error": "fee_ppm must be an integer"})
    );

    let negative = json!(-5);
    let v = set_fee_response(sources(
        &gate,
        Some(&channel),
        Some(&negative),
        false,
        Some(&setter),
        &limiter,
    ));
    assert_eq!(
        v,
        json!({"status": "error", "error": "fee_ppm must be non-negative"})
    );

    let boolean = json!(true);
    let v = set_fee_response(sources(
        &gate,
        Some(&channel),
        Some(&boolean),
        false,
        Some(&setter),
        &limiter,
    ));
    assert_eq!(
        v["error"], "Fee 1 is outside configured range [10, 3000]. Use force=true to override.",
        "int(True) == 1, which is below min -> the range gate: {v:?}"
    );
}

/// py 4767-4776 (P1-012 + the format regexes): non-string channel_id,
/// then the three accepted formats and a 65-hex reject.
#[test]
fn channel_id_type_and_format_gates() {
    let gate = enabled_gate();
    let limiter = ForceRateLimiter::production();
    let fee = json!(100);
    let setter = |_: &str, fee_ppm: i64, _: bool| json!({"success": true, "fee_ppm": fee_ppm});

    let numeric = json!(12345);
    let v = set_fee_response(sources(
        &gate,
        Some(&numeric),
        Some(&fee),
        false,
        Some(&setter),
        &limiter,
    ));
    assert_eq!(
        v,
        json!({"status": "error", "error": "channel_id must be a string"})
    );

    let bad = json!("not-a-channel");
    let v = set_fee_response(sources(
        &gate,
        Some(&bad),
        Some(&fee),
        false,
        Some(&setter),
        &limiter,
    ));
    assert_eq!(
        v,
        json!({"status": "error", "error": "Invalid channel_id or node_id format"})
    );

    for good in [
        json!("123x456x0"),
        json!("123:456:0"),
        json!("a".repeat(64)),
        json!("02".repeat(33)),
    ] {
        let v = set_fee_response(sources(
            &gate,
            Some(&good),
            Some(&fee),
            false,
            Some(&setter),
            &limiter,
        ));
        assert_eq!(v["status"], "success", "{good:?} -> {v:?}");
    }
    let hex65 = json!("a".repeat(65));
    let v = set_fee_response(sources(
        &gate,
        Some(&hex65),
        Some(&fee),
        false,
        Some(&setter),
        &limiter,
    ));
    assert_eq!(v["status"], "error");
}

/// py 4779-4784: without force, out-of-range fees are refused with the
/// exact f-string; py 4791-4800: WITH force the hard rails still clamp —
/// a 5 ppm force request reaches the setter as min (10), with
/// enforce_limits=false and the clamp disclosed; a 200k force request
/// clamps to min(max_fee_ppm, ABS_MAX 100_000) = 3000 here.
#[test]
fn range_gate_without_force_and_hard_rails_under_force() {
    let gate = enabled_gate();
    let limiter = ForceRateLimiter::production();
    let channel = json!("100x1x0");
    let seen: RefCell<Vec<(i64, bool)>> = RefCell::new(Vec::new());
    let setter = |_: &str, fee_ppm: i64, enforce: bool| {
        seen.borrow_mut().push((fee_ppm, enforce));
        json!({"success": true, "fee_ppm": fee_ppm})
    };

    let low = json!(5);
    let v = set_fee_response(sources(
        &gate,
        Some(&channel),
        Some(&low),
        false,
        Some(&setter),
        &limiter,
    ));
    assert_eq!(
        v["error"],
        "Fee 5 is outside configured range [10, 3000]. Use force=true to override."
    );
    assert!(
        seen.borrow().is_empty(),
        "the refusal never reaches the setter"
    );

    let v = set_fee_response(sources(
        &gate,
        Some(&channel),
        Some(&low),
        true,
        Some(&setter),
        &limiter,
    ));
    assert_eq!(v["status"], "success");
    assert_eq!(v["new_fee_ppm"], 10, "forced below min is RAISED to min");
    assert_eq!(v["requested_fee_ppm"], 5);
    assert_eq!(v["clamped_to_rail"], json!([10, 3000]));
    assert_eq!(
        seen.borrow().last(),
        Some(&(10, false)),
        "enforce_limits=(not force)"
    );

    let huge = json!(200_000);
    let v = set_fee_response(sources(
        &gate,
        Some(&channel),
        Some(&huge),
        true,
        Some(&setter),
        &limiter,
    ));
    assert_eq!(
        v["new_fee_ppm"], 3000,
        "forced above max clamps to the rail"
    );
    assert_eq!(v["requested_fee_ppm"], 200_000);

    // An in-range non-force set carries NO clamp disclosure keys.
    let fine = json!(500);
    let v = set_fee_response(sources(
        &gate,
        Some(&channel),
        Some(&fine),
        false,
        Some(&setter),
        &limiter,
    ));
    assert_eq!(v["status"], "success");
    assert!(v.get("requested_fee_ppm").is_none());
    assert!(v.get("clamped_to_rail").is_none());
    assert_eq!(seen.borrow().last(), Some(&(500, true)));

    // P1-004: ABS_MAX_FEE_PPM (100_000) caps the rail even when
    // max_fee_ppm is configured ABOVE it — a forced 150k against a 200k
    // config still clamps to 100k.
    let mut wide = sources(
        &gate,
        Some(&channel),
        Some(&fine),
        true,
        Some(&setter),
        &limiter,
    );
    wide.max_fee_ppm = 200_000;
    let big = json!(150_000);
    wide.fee_ppm = Some(&big);
    let v = set_fee_response(wide);
    assert_eq!(
        v["new_fee_ppm"], 100_000,
        "the absolute ceiling binds: {v:?}"
    );
    assert_eq!(v["clamped_to_rail"], json!([10, 100_000]));
}

/// py 4803-4815: the controller result dict merges Python-style. Failure:
/// `{"status": "error", "error": message or "Fee update failed",
/// **result}`; success: resolved channel/fee from the result, extras
/// carried through.
#[test]
fn controller_result_dicts_merge_python_style() {
    let gate = enabled_gate();
    let limiter = ForceRateLimiter::production();
    let channel = json!("100x1x0");
    let fee = json!(500);

    let failing = |_: &str, _: i64, _: bool| json!({"success": false, "message": "peer disconnected", "attempts": 2});
    let v = set_fee_response(sources(
        &gate,
        Some(&channel),
        Some(&fee),
        false,
        Some(&failing),
        &limiter,
    ));
    assert_eq!(v["status"], "error");
    assert_eq!(v["error"], "peer disconnected");
    assert_eq!(v["attempts"], 2, "**result extras survive the merge");
    assert_eq!(v["success"], false);

    let blank_message = |_: &str, _: i64, _: bool| json!({"success": false, "message": ""});
    let v = set_fee_response(sources(
        &gate,
        Some(&channel),
        Some(&fee),
        false,
        Some(&blank_message),
        &limiter,
    ));
    assert_eq!(
        v["error"], "Fee update failed",
        "py `or` treats '' as falsy"
    );

    let resolving = |_: &str, fee_ppm: i64, _: bool| {
        json!({
            "success": true,
            "fee_ppm": fee_ppm + 1,
            "channel_id": "resolved-scid",
            "note": "resolved from node id",
        })
    };
    let v = set_fee_response(sources(
        &gate,
        Some(&channel),
        Some(&fee),
        false,
        Some(&resolving),
        &limiter,
    ));
    assert_eq!(v["status"], "success");
    assert_eq!(
        v["channel"], "resolved-scid",
        "py takes result['channel_id']"
    );
    assert_eq!(v["new_fee_ppm"], 501, "py takes result['fee_ppm']");
    assert_eq!(v["note"], "resolved from node id");
}
