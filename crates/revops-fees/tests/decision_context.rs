use revops_fees::cycle::{DecisionClock, FixedDecisionClock};
use revops_fees::execution::{
    FeeAuthorizationRequest, FeeAuthorizationResult, FeeAuthorizer, GovernedDeps,
    GovernedFeeAuthorizer, GovernedTrace,
};
use revops_fees::pyrand::{DecisionEntropy, DecisionInputError, PyRandom};
use revops_fees::thompson::sampling::{
    sample_fee_contextual_with_entropy_and_clock, sample_fee_with_entropy,
};
use revops_fees::thompson::{GaussianThompsonState, Observation};
use revops_fees::vegas::{vegas_update_with_entropy, VegasReflexState};

#[derive(Default)]
struct RecordingEntropy {
    calls: Vec<String>,
    gauss_values: std::collections::VecDeque<f64>,
    random_values: std::collections::VecDeque<f64>,
    fail_label: Option<String>,
}

impl DecisionEntropy for RecordingEntropy {
    fn random(&mut self, label: &str) -> Result<f64, DecisionInputError> {
        self.calls.push(label.to_string());
        if self.fail_label.as_deref() == Some(label) {
            return Err(DecisionInputError::new(format!("missing entropy: {label}")));
        }
        self.random_values
            .pop_front()
            .ok_or_else(|| DecisionInputError::new(format!("missing entropy: {label}")))
    }

    fn gauss(&mut self, label: &str, _mu: f64, _sigma: f64) -> Result<f64, DecisionInputError> {
        self.calls.push(label.to_string());
        if self.fail_label.as_deref() == Some(label) {
            return Err(DecisionInputError::new(format!("missing entropy: {label}")));
        }
        self.gauss_values
            .pop_front()
            .ok_or_else(|| DecisionInputError::new(format!("missing entropy: {label}")))
    }
}

#[test]
fn production_entropy_requires_non_empty_labels_without_advancing_the_stream() {
    let mut actual = PyRandom::seed_from_u64(20260719);
    let error = DecisionEntropy::random(&mut actual, "").expect_err("empty labels fail closed");
    assert_eq!(error, DecisionInputError::empty_label("entropy"));

    let mut expected = PyRandom::seed_from_u64(20260719);
    assert_eq!(
        DecisionEntropy::random(&mut actual, "after.error").unwrap(),
        expected.random(),
        "label validation must happen before consuming entropy"
    );
}

#[test]
fn production_entropy_preserves_random_and_gaussian_cache_sequences() {
    let mut actual = PyRandom::seed_from_u64(7);
    let mut expected = PyRandom::seed_from_u64(7);

    assert_eq!(
        DecisionEntropy::gauss(&mut actual, "first.gauss", 10.0, 2.0).unwrap(),
        expected.gauss(10.0, 2.0)
    );
    assert_eq!(
        DecisionEntropy::random(&mut actual, "middle.random").unwrap(),
        expected.random()
    );
    assert_eq!(
        DecisionEntropy::gauss(&mut actual, "cached.gauss", -5.0, 3.0).unwrap(),
        expected.gauss(-5.0, 3.0)
    );
}

#[test]
fn vegas_preserves_short_circuit_and_uses_python_label() {
    let mut state = VegasReflexState::default();
    let mut entropy = RecordingEntropy {
        random_values: [0.0].into(),
        ..RecordingEntropy::default()
    };
    vegas_update_with_entropy(&mut state, 3.0, 1.0, &mut entropy, 7777).unwrap();
    assert_eq!(entropy.calls, ["vegas.boost"]);

    state.consecutive_spikes = 1;
    entropy.calls.clear();
    vegas_update_with_entropy(&mut state, 3.0, 1.0, &mut entropy, 8888).unwrap();
    assert!(entropy.calls.is_empty(), "confirmed spike must not draw");
}

#[test]
fn thompson_sparse_and_polynomial_paths_use_python_labels_in_order() {
    let mut sparse = GaussianThompsonState::default();
    let mut entropy = RecordingEntropy {
        gauss_values: [200.0].into(),
        ..RecordingEntropy::default()
    };
    sample_fee_with_entropy(&mut sparse, 10, 500, None, &mut entropy, 1234).unwrap();
    assert_eq!(entropy.calls, ["thompson.prior"]);

    let mut polynomial = GaussianThompsonState {
        observations: (0..5)
            .map(|_| Observation::new(200.0, 1.0, 1.0, 1, "normal"))
            .collect(),
        last_fee_min: 100.0,
        last_fee_max: 300.0,
        posterior_precision: [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
        posterior_coeffs: [-1.0, 0.0, 0.0],
        ..GaussianThompsonState::default()
    };
    let mut entropy = RecordingEntropy {
        gauss_values: [0.1, 0.2, 0.3].into(),
        ..RecordingEntropy::default()
    };
    sample_fee_with_entropy(&mut polynomial, 10, 500, None, &mut entropy, 1234).unwrap();
    assert_eq!(
        entropy.calls,
        [
            "thompson.polynomial.coefficient.0",
            "thompson.polynomial.coefficient.1",
            "thompson.polynomial.coefficient.2",
        ]
    );
}

#[test]
fn entropy_errors_propagate_without_substitution() {
    let mut state = GaussianThompsonState::default();
    let mut entropy = RecordingEntropy {
        fail_label: Some("thompson.prior".to_string()),
        ..RecordingEntropy::default()
    };
    let error = sample_fee_with_entropy(&mut state, 10, 500, None, &mut entropy, 1234)
        .expect_err("strict entropy exhaustion must fail closed");
    assert_eq!(error.to_string(), "missing entropy: thompson.prior");
}

#[test]
fn thompson_sampling_consumes_bias_clock_before_sample_clock() {
    let mut state = GaussianThompsonState {
        posterior_bias: vec![(300.0, 0.5, 1_000)],
        ..GaussianThompsonState::default()
    };
    let mut entropy = RecordingEntropy {
        gauss_values: [200.0].into(),
        ..RecordingEntropy::default()
    };
    let mut labels = Vec::new();
    let mut values: std::collections::VecDeque<i64> = [2_000, 2_001].into();
    sample_fee_contextual_with_entropy_and_clock(
        &mut state,
        "missing",
        10,
        500,
        None,
        &mut entropy,
        &mut |label| {
            labels.push(label.to_string());
            values
                .pop_front()
                .ok_or_else(|| DecisionInputError::new(format!("missing clock: {label}")))
        },
    )
    .unwrap();

    assert_eq!(
        labels,
        ["thompson.posterior_bias.shift", "thompson.last_sample_time",]
    );
    assert_eq!(state.last_sample_time, 2_001);
}

#[derive(Default)]
struct RecordingClock {
    values: std::collections::VecDeque<i64>,
    labels: Vec<String>,
}

impl DecisionClock for RecordingClock {
    fn now(&mut self, label: &str) -> Result<i64, DecisionInputError> {
        self.labels.push(label.to_string());
        self.values
            .pop_front()
            .ok_or_else(|| DecisionInputError::new(format!("missing clock: {label}")))
    }
}

#[test]
fn fixed_decision_clock_reuses_one_scheduler_value_and_validates_labels() {
    let mut clock = FixedDecisionClock::new(1_752_400_000);
    assert_eq!(clock.now("cycle.started_at").unwrap(), 1_752_400_000);
    assert_eq!(clock.now("cycle.channel.evaluate").unwrap(), 1_752_400_000);
    assert_eq!(
        clock.now("").expect_err("empty clock label fails closed"),
        DecisionInputError::empty_label("clock")
    );
}

#[test]
fn scripted_clock_preserves_semantic_call_order_and_exhaustion() {
    let mut clock = RecordingClock {
        values: [10, 11, 12].into(),
        labels: Vec::new(),
    };
    assert_eq!(clock.now("cycle.started_at").unwrap(), 10);
    assert_eq!(clock.now("cycle.channel.evaluate").unwrap(), 11);
    assert_eq!(clock.now("pid.calculate").unwrap(), 12);
    let error = clock
        .now("fee.apply")
        .expect_err("strict clock exhaustion must fail closed");
    assert_eq!(error.to_string(), "missing clock: fee.apply");
    assert_eq!(
        clock.labels,
        [
            "cycle.started_at",
            "cycle.channel.evaluate",
            "pid.calculate",
            "fee.apply",
        ]
    );
}

struct ScriptedAuthorizer {
    result: FeeAuthorizationResult,
}

impl FeeAuthorizer for ScriptedAuthorizer {
    fn authorize(
        &self,
        _request: &FeeAuthorizationRequest,
    ) -> Result<FeeAuthorizationResult, DecisionInputError> {
        Ok(self.result.clone())
    }
}

fn authorization_request() -> FeeAuthorizationRequest {
    FeeAuthorizationRequest {
        channel_id: "820x1x0".to_string(),
        fee_ppm: 150,
        old_fee_ppm: Some(100),
        reason: "DTS+PID: test".to_string(),
        reason_code: Some("dts_pid_sample".to_string()),
        now: 1_752_400_000,
    }
}

#[test]
fn scripted_authorizer_needs_no_ledger_or_intent_registry() {
    let expected = FeeAuthorizationResult {
        authorized: true,
        reason_code: "scripted".to_string(),
        trace: Some(GovernedTrace {
            authorized: true,
            reason_code: "scripted".to_string(),
            intent_id: "script-intent".to_string(),
            idempotency_key: "script-key".to_string(),
        }),
    };
    let authorizer = ScriptedAuthorizer {
        result: expected.clone(),
    };
    assert_eq!(
        authorizer.authorize(&authorization_request()).unwrap(),
        expected
    );
}

#[test]
fn production_authorizer_preserves_trace_and_ledger_behavior() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let ledger =
        revops_econ::ledger::EconLedger::open(dir.path().join("econ_ledger.db")).expect("ledger");
    let deps = GovernedDeps {
        ledger: Some(&ledger),
        registry: None,
        paused: false,
        authority_level: Some("capital".to_string()),
    };
    let authorizer = GovernedFeeAuthorizer::new(&deps);
    let result = authorizer.authorize(&authorization_request()).unwrap();
    assert!(result.authorized, "{}", result.reason_code);
    assert!(result.trace.expect("trace").intent_id.starts_with("int-"));
    assert_eq!(ledger.count_events(Some("intent_proposed")).unwrap(), 1);
    assert_eq!(ledger.count_events(Some("intent_authorized")).unwrap(), 1);
    assert_eq!(ledger.count_events(Some("budget_reserved")).unwrap(), 0);
}
