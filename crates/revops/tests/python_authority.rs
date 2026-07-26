//! Tests for Task 8 step 2/3: Python-authority-status validation
//! (`revops::python_authority`). The `revenue-fee-authority-status` RPC
//! method may not exist on the node yet (companion Python handoff plan) --
//! per this task's ruling, all validation logic here is exercised against
//! injected/fixture JSON responses, never a live RPC call.

use revops::python_authority::{
    validate_stable_epoch, validate_status, PythonAuthorityDenyReason, PythonAuthorityOff,
    AUTHORITY_STATUS_METHOD,
};
use serde_json::json;

const MAX_AGE: i64 = 30;

fn valid_response(now: i64) -> serde_json::Value {
    json!({
        "enabled": false,
        "generation": 3,
        "transitioned_at": now - 100,
        "observed_at": now - 1,
    })
}

// ---------------------------------------------------------------------------
// Success case
// ---------------------------------------------------------------------------

#[test]
fn valid_off_response_parses_into_python_authority_off() {
    let now = 2_000_000;
    let raw = valid_response(now);
    let status = validate_status(&raw, now, MAX_AGE).expect("valid off response must parse");
    assert_eq!(
        status,
        PythonAuthorityOff {
            generation: 3,
            transitioned_at: now - 100,
            observed_at: now - 1,
        }
    );
}

// ---------------------------------------------------------------------------
// Exact schema: missing / wrong-typed fields
// ---------------------------------------------------------------------------

#[test]
fn non_object_response_is_malformed() {
    let now = 2_000_000;
    let raw = json!([1, 2, 3]);
    let err = validate_status(&raw, now, MAX_AGE).unwrap_err();
    assert!(matches!(
        err,
        PythonAuthorityDenyReason::MalformedResponse(_)
    ));
}

#[test]
fn missing_enabled_field_is_denied() {
    let now = 2_000_000;
    let mut raw = valid_response(now);
    raw.as_object_mut().unwrap().remove("enabled");
    let err = validate_status(&raw, now, MAX_AGE).unwrap_err();
    assert_eq!(err, PythonAuthorityDenyReason::MissingField("enabled"));
}

#[test]
fn missing_generation_field_is_denied() {
    let now = 2_000_000;
    let mut raw = valid_response(now);
    raw.as_object_mut().unwrap().remove("generation");
    let err = validate_status(&raw, now, MAX_AGE).unwrap_err();
    assert_eq!(err, PythonAuthorityDenyReason::MissingField("generation"));
}

#[test]
fn missing_transitioned_at_field_is_denied() {
    let now = 2_000_000;
    let mut raw = valid_response(now);
    raw.as_object_mut().unwrap().remove("transitioned_at");
    let err = validate_status(&raw, now, MAX_AGE).unwrap_err();
    assert_eq!(
        err,
        PythonAuthorityDenyReason::MissingField("transitioned_at")
    );
}

#[test]
fn missing_observed_at_field_is_denied() {
    let now = 2_000_000;
    let mut raw = valid_response(now);
    raw.as_object_mut().unwrap().remove("observed_at");
    let err = validate_status(&raw, now, MAX_AGE).unwrap_err();
    assert_eq!(err, PythonAuthorityDenyReason::MissingField("observed_at"));
}

#[test]
fn enabled_as_string_is_wrong_field_type_not_coerced() {
    let now = 2_000_000;
    let mut raw = valid_response(now);
    raw["enabled"] = json!("false");
    let err = validate_status(&raw, now, MAX_AGE).unwrap_err();
    assert_eq!(err, PythonAuthorityDenyReason::WrongFieldType("enabled"));
}

#[test]
fn generation_as_float_is_wrong_field_type() {
    let now = 2_000_000;
    let mut raw = valid_response(now);
    raw["generation"] = json!(3.5);
    let err = validate_status(&raw, now, MAX_AGE).unwrap_err();
    assert_eq!(err, PythonAuthorityDenyReason::WrongFieldType("generation"));
}

// ---------------------------------------------------------------------------
// enabled=false requirement
// ---------------------------------------------------------------------------

#[test]
fn enabled_true_is_denied() {
    let now = 2_000_000;
    let mut raw = valid_response(now);
    raw["enabled"] = json!(true);
    let err = validate_status(&raw, now, MAX_AGE).unwrap_err();
    assert_eq!(err, PythonAuthorityDenyReason::StillEnabled);
}

// ---------------------------------------------------------------------------
// Nonnegative generation/timestamps
// ---------------------------------------------------------------------------

#[test]
fn negative_generation_is_denied() {
    let now = 2_000_000;
    let mut raw = valid_response(now);
    raw["generation"] = json!(-1);
    let err = validate_status(&raw, now, MAX_AGE).unwrap_err();
    assert_eq!(err, PythonAuthorityDenyReason::NegativeField("generation"));
}

#[test]
fn negative_transitioned_at_is_denied() {
    let now = 2_000_000;
    let mut raw = valid_response(now);
    raw["transitioned_at"] = json!(-5);
    let err = validate_status(&raw, now, MAX_AGE).unwrap_err();
    assert_eq!(
        err,
        PythonAuthorityDenyReason::NegativeField("transitioned_at")
    );
}

#[test]
fn negative_observed_at_is_denied() {
    let now = 2_000_000;
    let mut raw = valid_response(now);
    raw["observed_at"] = json!(-5);
    let err = validate_status(&raw, now, MAX_AGE).unwrap_err();
    assert_eq!(err, PythonAuthorityDenyReason::NegativeField("observed_at"));
}

// ---------------------------------------------------------------------------
// Bounded observation age
// ---------------------------------------------------------------------------

#[test]
fn observation_older_than_bound_is_stale() {
    let now = 2_000_000;
    let mut raw = valid_response(now);
    raw["observed_at"] = json!(now - MAX_AGE - 1);
    let err = validate_status(&raw, now, MAX_AGE).unwrap_err();
    assert!(matches!(
        err,
        PythonAuthorityDenyReason::StaleObservation { .. }
    ));
}

#[test]
fn observation_exactly_at_bound_is_accepted() {
    let now = 2_000_000;
    let mut raw = valid_response(now);
    raw["observed_at"] = json!(now - MAX_AGE);
    validate_status(&raw, now, MAX_AGE).expect("boundary age must be accepted (inclusive)");
}

#[test]
fn observation_from_the_future_is_denied() {
    let now = 2_000_000;
    let mut raw = valid_response(now);
    raw["observed_at"] = json!(now + 10);
    let err = validate_status(&raw, now, MAX_AGE).unwrap_err();
    assert!(matches!(
        err,
        PythonAuthorityDenyReason::StaleObservation { .. }
    ));
}

// ---------------------------------------------------------------------------
// Stable transition epoch across a batch acquisition
// ---------------------------------------------------------------------------

#[test]
fn matching_epoch_across_two_reads_is_stable() {
    let first = PythonAuthorityOff {
        generation: 5,
        transitioned_at: 1_000,
        observed_at: 1_001,
    };
    let second = PythonAuthorityOff {
        generation: 5,
        transitioned_at: 1_000,
        // observed_at may differ (a fresh read) -- only generation and
        // transitioned_at must remain stable.
        observed_at: 1_050,
    };
    validate_stable_epoch(&first, &second).expect("identical epoch must be stable");
}

#[test]
fn generation_change_between_reads_is_unstable_epoch() {
    let first = PythonAuthorityOff {
        generation: 5,
        transitioned_at: 1_000,
        observed_at: 1_001,
    };
    let second = PythonAuthorityOff {
        generation: 6,
        transitioned_at: 1_000,
        observed_at: 1_050,
    };
    let err = validate_stable_epoch(&first, &second).unwrap_err();
    assert!(matches!(
        err,
        PythonAuthorityDenyReason::UnstableEpoch { .. }
    ));
}

#[test]
fn transitioned_at_change_between_reads_is_unstable_epoch() {
    let first = PythonAuthorityOff {
        generation: 5,
        transitioned_at: 1_000,
        observed_at: 1_001,
    };
    let second = PythonAuthorityOff {
        generation: 5,
        transitioned_at: 1_200,
        observed_at: 1_250,
    };
    let err = validate_stable_epoch(&first, &second).unwrap_err();
    assert!(matches!(
        err,
        PythonAuthorityDenyReason::UnstableEpoch { .. }
    ));
}

// ---------------------------------------------------------------------------
// RPC method presence / classification (no live socket needed)
// ---------------------------------------------------------------------------

#[test]
fn authority_status_method_name_is_the_python_contract_name() {
    assert_eq!(AUTHORITY_STATUS_METHOD, "revenue-fee-authority-status");
}

#[test]
fn method_not_found_style_rpc_error_is_classified_distinctly() {
    use revops::python_authority::classify_rpc_proxy_error;
    use revops_rpc::RpcProxyError;

    let err = RpcProxyError::Rpc(anyhow::anyhow!(
        "revenue-fee-authority-status RPC error: Error code -32601: Unknown command: \
         revenue-fee-authority-status"
    ));
    let classified = classify_rpc_proxy_error(&err);
    assert_eq!(classified, PythonAuthorityDenyReason::MethodNotFound);
}

#[test]
fn generic_rpc_error_is_classified_as_transport() {
    use revops::python_authority::classify_rpc_proxy_error;
    use revops_rpc::RpcProxyError;

    let err = RpcProxyError::Rpc(anyhow::anyhow!(
        "revenue-fee-authority-status RPC error: connection refused"
    ));
    let classified = classify_rpc_proxy_error(&err);
    assert!(matches!(
        classified,
        PythonAuthorityDenyReason::Transport(_)
    ));
}

#[test]
fn timeout_is_classified_as_timeout() {
    use revops::python_authority::classify_rpc_proxy_error;
    use revops_rpc::RpcProxyError;

    let err = RpcProxyError::Timeout {
        method: AUTHORITY_STATUS_METHOD.to_string(),
        seconds: 15,
    };
    let classified = classify_rpc_proxy_error(&err);
    assert_eq!(
        classified,
        PythonAuthorityDenyReason::Timeout { seconds: 15 }
    );
}

// ---------------------------------------------------------------------------
// Structural read-only client: only one callable RPC method
// ---------------------------------------------------------------------------

#[test]
fn client_constructs_over_a_socket_path_and_timeout() {
    use revops::python_authority::PythonAuthorityClient;
    // Construction must not touch the filesystem or a socket -- this must
    // succeed even though the path does not exist.
    let _client =
        PythonAuthorityClient::new(std::path::PathBuf::from("/nonexistent/lightning-rpc"), 15);
}
