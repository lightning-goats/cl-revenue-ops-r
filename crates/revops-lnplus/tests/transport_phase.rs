//! Task 61 4C — the transport-phase contract that feeds 4B's typed
//! submission outcomes: a transport failure BEFORE the request was sent is
//! a clean (known not-submitted) failure; a failure AFTER the request may
//! have been sent — and any 2xx response the client cannot interpret — is
//! OUTCOME UNKNOWN on the resulting `LnPlusError`, so the kernel
//! quarantines instead of falsifying a clean failure.

mod common;

use common::{FakeHttpTransport, FakeSigner};
use revops_lnplus::http::{LnPlusApiClient, TransportError, TransportPhase};
use revops_lnplus::ports::LnPlusApi;

fn client<'a>(
    t: &'a FakeHttpTransport,
    s: &'a FakeSigner,
) -> LnPlusApiClient<&'a FakeHttpTransport, &'a FakeSigner> {
    LnPlusApiClient::with_base_url(t, s, "http://127.0.0.1:1/api/2")
}

/// Auth challenge the client fetches before every signed call.
fn queue_challenge(t: &FakeHttpTransport) {
    t.push_json(serde_json::json!({
        "message": "lnplus login 1234"
    }));
}

#[test]
fn transport_failure_after_send_marks_outcome_unknown() {
    let t = FakeHttpTransport::new();
    let s = FakeSigner::new();
    queue_challenge(&t);
    t.responses
        .borrow_mut()
        .push_back(Err(TransportError::after_send(
            "read timeout awaiting response",
        )));
    let err = client(&t, &s).create_application("s1").unwrap_err();
    assert!(
        err.is_outcome_unknown(),
        "a post-send transport failure must be typed OUTCOME UNKNOWN: {err:?}"
    );
}

#[test]
fn transport_failure_before_send_is_a_clean_known_failure() {
    let t = FakeHttpTransport::new();
    let s = FakeSigner::new();
    queue_challenge(&t);
    t.responses
        .borrow_mut()
        .push_back(Err(TransportError::before_send("connection refused")));
    let err = client(&t, &s).create_application("s1").unwrap_err();
    assert!(
        !err.is_outcome_unknown(),
        "a pre-send failure is KNOWN not-submitted: {err:?}"
    );
}

#[test]
fn challenge_fetch_failure_is_always_clean_regardless_of_phase() {
    // The get_message challenge round-trip carries no mutation — even an
    // after-send failure THERE means the mutating request was never
    // issued. The client must not leak the challenge's phase into the
    // mutating call's outcome.
    let t = FakeHttpTransport::new();
    let s = FakeSigner::new();
    t.responses
        .borrow_mut()
        .push_back(Err(TransportError::after_send("reset during challenge")));
    let err = client(&t, &s).create_application("s1").unwrap_err();
    assert!(
        !err.is_outcome_unknown(),
        "a failure during the auth challenge can never make the mutation unknown: {err:?}"
    );
    assert_eq!(t.call_count(), 1, "the mutating request was never issued");
}

#[test]
fn uninterpretable_2xx_response_marks_outcome_unknown() {
    // The server said 200 — the action very likely happened — but the body
    // is not JSON. Treating this as a clean failure would invite a
    // resubmit of an already-committed action.
    let t = FakeHttpTransport::new();
    let s = FakeSigner::new();
    queue_challenge(&t);
    t.push_raw_status(200, b"<html>gateway mangled this</html>");
    let err = client(&t, &s).create_application("s1").unwrap_err();
    assert!(err.is_outcome_unknown(), "2xx + unparseable body: {err:?}");
}

#[test]
fn oversized_2xx_response_marks_outcome_unknown() {
    let t = FakeHttpTransport::new();
    let s = FakeSigner::new();
    queue_challenge(&t);
    let huge = vec![b'x'; revops_lnplus::http::MAX_RESPONSE_BYTES + 1];
    t.push_raw_status(200, &huge);
    let err = client(&t, &s).create_application("s1").unwrap_err();
    assert!(err.is_outcome_unknown(), "2xx over the size cap: {err:?}");
}

#[test]
fn structured_non_2xx_stays_a_clean_known_refusal() {
    // CONTROL: LN+ answered with a refusal — the outcome is KNOWN (not
    // submitted / rejected), never unknown.
    let t = FakeHttpTransport::new();
    let s = FakeSigner::new();
    queue_challenge(&t);
    t.push_status(
        422,
        serde_json::json!({"errors": {"id": ["already applied"]}}),
    );
    let err = client(&t, &s).create_application("s1").unwrap_err();
    assert!(
        !err.is_outcome_unknown(),
        "structured refusal is known: {err:?}"
    );
    assert_eq!(err.http_status, Some(422));
}

#[test]
fn phase_constructors_roundtrip() {
    assert_eq!(
        TransportError::before_send("x").phase,
        TransportPhase::BeforeRequestSent
    );
    assert_eq!(
        TransportError::after_send("x").phase,
        TransportPhase::AfterRequestSent
    );
}

#[test]
fn exact_at_cap_2xx_response_is_accepted() {
    // Boundary pin (4C correction gate): exactly MAX_RESPONSE_BYTES is
    // fine; the unknown-typed rejection starts at cap+1 (previous test).
    let t = FakeHttpTransport::new();
    let s = FakeSigner::new();
    queue_challenge(&t);
    let mut body = b"{}".to_vec();
    body.resize(revops_lnplus::http::MAX_RESPONSE_BYTES, b' ');
    assert_eq!(body.len(), revops_lnplus::http::MAX_RESPONSE_BYTES);
    t.push_raw_status(200, &body);
    client(&t, &s)
        .create_application("s1")
        .expect("an exactly-at-cap valid response must be accepted");
}
