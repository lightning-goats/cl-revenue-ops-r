use revops_fees::cycle::FeeCfgSnapshot;
use revops_fees::execution::{
    FeeExecutionRequest, FeeExecutor, RecordingFeeExecutor, SetChannelRequest, SetFeeRequest,
};
use revops_fees::pyjson::OValue;
use serde_json::json;

fn execution_request(
    fee_ppm: i64,
    base_fee_msat: i64,
    htlcmin_msat: Option<i64>,
    htlcmax_msat: Option<i64>,
) -> FeeExecutionRequest {
    FeeExecutionRequest {
        decision: SetFeeRequest {
            channel_id: "123x4x5".to_string(),
            fee_ppm,
            enforce_limits: true,
            effective_min_fee_ppm: None,
            htlcmin_msat,
            htlcmax_msat,
            base_fee_msat,
        },
        wire_request: OValue::Null,
        old_fee_ppm: 111,
        expected_base_fee_msat: base_fee_msat,
    }
}

#[test]
fn setchannel_serializes_all_fields_exactly() {
    let request = SetChannelRequest::try_from_execution(&execution_request(
        321,
        0,
        Some(1_000),
        Some(4_000_000),
    ))
    .expect("valid setchannel request");

    assert_eq!(request.id, "123x4x5");
    assert_eq!(
        request.to_params(),
        json!({
            "id": "123x4x5",
            "feebase": 0,
            "feeppm": 321,
            "htlcmin": 1_000,
            "htlcmax": 4_000_000,
        })
    );
}

#[test]
fn setchannel_omits_absent_optional_fields() {
    let request = SetChannelRequest::try_from_execution(&execution_request(321, 7, None, None))
        .expect("valid setchannel request");
    let params = request.to_params();

    assert_eq!(
        params,
        json!({
            "id": "123x4x5",
            "feebase": 7,
            "feeppm": 321,
        })
    );
    assert!(!params.as_object().unwrap().contains_key("htlcmin"));
    assert!(!params.as_object().unwrap().contains_key("htlcmax"));
}

#[test]
fn setchannel_rejects_negative_and_overflowing_wire_integers() {
    let cases = [
        (execution_request(1, -1, None, None), "feebase"),
        (execution_request(-1, 0, None, None), "feeppm"),
        (
            execution_request(i64::from(u32::MAX) + 1, 0, None, None),
            "feeppm",
        ),
        (execution_request(1, 0, Some(-1), None), "htlcmin"),
        (execution_request(1, 0, None, Some(-1)), "htlcmax"),
    ];

    for (request, field) in cases {
        let error = SetChannelRequest::try_from_execution(&request)
            .expect_err("invalid wire integer must fail closed");
        assert!(error.to_string().contains(field), "{field}: {error}");
    }
}

#[test]
fn setchannel_recording_executor_records_the_successful_clamped_intent() {
    let executor = RecordingFeeExecutor::default();
    let cfg = FeeCfgSnapshot {
        min_fee_ppm: 10,
        max_fee_ppm: 5_000,
        ..FeeCfgSnapshot::default()
    };
    let request = execution_request(999_999, 12, Some(1_000), Some(4_000_000));

    let decision = executor
        .execute(&request, &cfg, None)
        .expect("pure execution");
    let actions = executor.recorded_actions();

    assert_eq!(decision.clamped_fee_ppm, 5_000);
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].decision, decision);
    assert_eq!(actions[0].old_fee_ppm, 111);
    assert_eq!(actions[0].expected_base_fee_msat, 12);
    assert_eq!(
        actions[0].request.to_params(),
        json!({
            "id": "123x4x5",
            "feebase": 12,
            "feeppm": 5_000,
            "htlcmin": 1_000,
            "htlcmax": 4_000_000,
        })
    );
}

#[test]
fn setchannel_recording_executor_omits_unsuccessful_decisions() {
    use revops_analytics::policy::{FeeStrategy, PeerPolicy};

    let executor = RecordingFeeExecutor::default();
    let mut policy = PeerPolicy::default_for("peer");
    policy.strategy = FeeStrategy::Passive;

    let decision = executor
        .execute(
            &execution_request(321, 0, None, None),
            &FeeCfgSnapshot::default(),
            Some(&policy),
        )
        .expect("pure execution");

    assert!(!decision.success);
    assert!(executor.recorded_actions().is_empty());
}

#[test]
fn setchannel_recording_executor_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<RecordingFeeExecutor>();
}
