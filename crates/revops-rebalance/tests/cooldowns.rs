//! Persisted cooldown / dest-cooldown (audit F7) / drift-override golden
//! parity, pinned by `fixtures/rebalance/cooldowns.json` (generated from
//! the REAL `modules/database.Database.record_pair_rebalance_failure` and
//! `modules/rebalance_engine_v2._effective_dest_cooldown_secs` by
//! `tools/port/gen_rebalance_fixtures.py cooldowns` in the port worktree,
//! branch `phase5-t6-gen`). `PairFutility`/`DestFutility` are proven by
//! inline unit tests in `src/cooldowns.rs` instead (see that module's doc
//! comment for why they are not fixture-driven).

use revops_rebalance::cooldowns::{dest_cooldown_secs, drift_override, persisted_cooldown_secs};
use revops_rebalance::errors::FailureKind;
use serde_json::Value;
use std::path::PathBuf;

fn fixture() -> Value {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/rebalance/cooldowns.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

/// Python's 7 `_pair_failure_cooldowns` string keys map onto Rust's 6
/// `FailureKind` variants — `local_execution_failed` and `other_retriable`
/// both merge into `LocalOrOther` (both base 600 in Python, per
/// `errors.rs`'s doc comment).
fn kind_from_str(s: &str) -> FailureKind {
    match s {
        "temporary_channel_failure" => FailureKind::TemporaryChannelFailure,
        "fee_insufficient" => FailureKind::FeeInsufficient,
        "incorrect_cltv_expiry" => FailureKind::IncorrectCltv,
        "permanent_failure" => FailureKind::Permanent,
        "payment_pending_timeout" => FailureKind::PaymentPendingTimeout,
        "local_execution_failed" | "other_retriable" => FailureKind::LocalOrOther,
        other => panic!("unmapped failure kind {other:?}"),
    }
}

#[test]
fn persisted_cooldown_secs_replays_python_fixture() {
    let fx = fixture();
    let cases = fx["persisted_cooldown_cases"].as_array().unwrap();
    assert_eq!(cases.len(), 7 * 8, "7 kinds x counts 1..=8");
    for case in cases {
        let kind_str = case["kind"].as_str().unwrap();
        let failure_count = case["failure_count"].as_i64().unwrap();
        let expected = case["expected_secs"].as_i64().unwrap();
        let kind = kind_from_str(kind_str);
        assert_eq!(
            persisted_cooldown_secs(kind, failure_count),
            expected,
            "kind={kind_str} count={failure_count}"
        );
    }
}

#[test]
fn dest_cooldown_secs_replays_python_fixture() {
    let fx = fixture();
    let cases = fx["dest_cooldown_cases"].as_array().unwrap();
    assert!(!cases.is_empty());
    for case in cases {
        let case_id = case["case_id"].as_str().unwrap();
        let base_secs = case["base_secs"].as_i64().unwrap();
        let amount_sats = case["amount_sats"].as_i64().unwrap();
        let remaining_gap = case["remaining_band_gap_sats"].as_i64().unwrap();
        let expected = case["expected_secs"].as_i64().unwrap();
        assert_eq!(
            dest_cooldown_secs(base_secs, amount_sats, remaining_gap),
            expected,
            "case {case_id}"
        );
    }
}

#[test]
fn drift_override_replays_python_fixture() {
    let fx = fixture();
    let cases = fx["drift_override_cases"].as_array().unwrap();
    assert!(!cases.is_empty());
    for case in cases {
        let case_id = case["case_id"].as_str().unwrap();
        let anchor_ratio = case["anchor_ratio"].as_f64().unwrap();
        let current_ratio = case["current_ratio"].as_f64().unwrap();
        let expected = case["expected"].as_bool().unwrap();
        assert_eq!(
            drift_override(anchor_ratio, current_ratio),
            expected,
            "case {case_id}"
        );
    }
}
