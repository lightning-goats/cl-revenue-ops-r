//! `LnPlusApiClient` — every test here runs against `FakeHttpTransport` /
//! `FakeSigner` (in-memory, `tests/common/mod.rs`). HARD RULE: no test in
//! this file performs a real network call — there is no concrete
//! `HttpTransport` impl anywhere in this crate to make one with (see
//! `http.rs`'s module doc).

mod common;

use common::*;
use revops_lnplus::http::{HttpMethod, LnPlusApiClient, MAX_RESPONSE_BYTES};
use revops_lnplus::ports::LnPlusApi;
use revops_lnplus::types::Rating;

fn client<'a>(
    transport: &'a FakeHttpTransport,
    signer: &'a FakeSigner,
) -> LnPlusApiClient<&'a FakeHttpTransport, &'a FakeSigner> {
    LnPlusApiClient::new(transport, signer)
}

fn queue_challenge(transport: &FakeHttpTransport, message: &str) {
    transport.push_json(serde_json::json!({"message": message}));
}

// --------------------------------------------------------------- auth flow

#[test]
fn auth_flow_fetches_challenge_signs_and_attaches_params() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "Please sign this LN+ auth challenge: abc123");
    transport.push_json(serde_json::json!({"swaps": []}));

    let c = client(&transport, &signer);
    let result = c.get_applicable_swaps();

    assert!(result.is_ok());
    assert_eq!(transport.call_count(), 2, "get_message then the real call");
    assert_eq!(
        signer.calls.borrow().as_slice(),
        ["Please sign this LN+ auth challenge: abc123"]
    );

    let calls = transport.calls.borrow();
    assert_eq!(calls[0].method, HttpMethod::Get);
    assert!(calls[0].url.ends_with("/get_message"));
    assert_eq!(calls[1].method, HttpMethod::Post);
    assert!(calls[1].url.ends_with("/get_applicable_swaps"));
    let body = String::from_utf8(calls[1].body.clone().unwrap()).unwrap();
    assert!(body.contains("message=Please"));
    assert!(body.contains("signature=zbase-signature"));
    // Content-Type header present for POST (control: GET must NOT set it).
    assert!(calls[1]
        .headers
        .iter()
        .any(|(k, v)| k == "Content-Type" && v == "application/x-www-form-urlencoded"));
    assert!(!calls[0].headers.iter().any(|(k, _)| k == "Content-Type"));
}

#[test]
fn gate_15_rejects_invoice_shaped_challenge_before_signing_or_a_second_request() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "lnbc1000n1invoiceshapedjunk");

    let c = client(&transport, &signer);
    let result = c.get_applicable_swaps();

    assert!(result.is_err());
    assert!(
        signer.calls.borrow().is_empty(),
        "an invoice-shaped challenge must never reach the signer"
    );
    assert_eq!(
        transport.call_count(),
        1,
        "only get_message — the real call must never fire"
    );
}

#[test]
fn control_plain_challenge_is_accepted_same_shape_different_text() {
    // CONTROL for the rejection test above: prove gate 15 is selective
    // (rejects invoice-shaped text specifically), not "always reject".
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "a perfectly ordinary challenge string");
    transport.push_json(serde_json::json!({"swaps": []}));

    let c = client(&transport, &signer);
    assert!(c.get_applicable_swaps().is_ok());
    assert_eq!(signer.calls.borrow().len(), 1);
}

#[test]
fn missing_zbase_signature_is_an_error_and_stops_before_the_real_call() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    *signer.result.borrow_mut() = Ok(String::new());
    queue_challenge(&transport, "a challenge");

    let c = client(&transport, &signer);
    let result = c.get_applicable_swaps();

    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("zbase"));
    assert_eq!(transport.call_count(), 1);
}

#[test]
fn signer_failure_surfaces_as_lnplus_error() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    *signer.result.borrow_mut() = Err(revops_lnplus::http::SignError("hsm locked".to_string()));
    queue_challenge(&transport, "a challenge");

    let c = client(&transport, &signer);
    let result = c.get_applicable_swaps();

    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("hsm locked"));
    assert_eq!(transport.call_count(), 1);
}

// --------------------------------------------------------- get_applicable_swaps

#[test]
fn get_applicable_swaps_parses_swaps_envelope() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "c");
    transport.push_json(serde_json::json!({
        "swaps": [{
            "id": 42,
            "status": "pending",
            "participant_waiting_for_count": 1,
            "capacity_sats": 2_000_000,
            "duration_months": 6,
            "participant_max_count": 3,
            "platform": "cln",
            "participants": [{
                "participant_identifier": "A",
                "pubkey": pubkey(1),
                "cancelled": false,
                "banned": false,
                "address_1": "1.2.3.4:9735",
                "address_2": null,
                "positive_ratings_count": 10,
                "negative_ratings_count": 1,
                "lnplus_rank_number": 3
            }]
        }]
    }));

    let c = client(&transport, &signer);
    let swaps = c.get_applicable_swaps().unwrap();

    assert_eq!(swaps.len(), 1);
    assert_eq!(swaps[0].id, "42", "numeric id normalized to string");
    assert_eq!(swaps[0].status, "pending");
    assert_eq!(swaps[0].capacity_sats, 2_000_000);
    assert_eq!(swaps[0].participants.len(), 1);
    assert_eq!(swaps[0].participants[0].pubkey, Some(pubkey(1)));
    assert_eq!(swaps[0].participants[0].negative_ratings_count, 1);
}

#[test]
fn get_applicable_swaps_accepts_bare_array_without_swaps_key() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "c");
    transport.push_json(serde_json::json!([
        {"id": "s1", "status": "pending", "participant_waiting_for_count": 1,
         "capacity_sats": 1, "duration_months": 1, "participant_max_count": 3,
         "platform": "cln", "participants": []}
    ]));

    let c = client(&transport, &signer);
    let swaps = c.get_applicable_swaps().unwrap();
    assert_eq!(swaps.len(), 1);
    assert_eq!(swaps[0].id, "s1");
}

// ------------------------------------------------------------------ get_swap

#[test]
fn get_swap_percent_encodes_the_id_and_unwraps_list_envelope() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    // A single-element list envelope, per LN+'s documented quirk.
    transport.push_json(serde_json::json!([{
        "id": "abc/def",
        "capacity_sats": 5_000_000,
        "duration_months": 12,
        "participants": []
    }]));

    let c = client(&transport, &signer);
    let detail = c.get_swap("abc/def").unwrap();

    assert_eq!(detail.id, "abc/def");
    assert_eq!(detail.capacity_sats, Some(5_000_000));
    let calls = transport.calls.borrow();
    assert_eq!(calls.len(), 1, "get_swap needs no auth round trip");
    assert_eq!(calls[0].method, HttpMethod::Get);
    assert!(
        calls[0].url.contains("get_swap/id=abc%2Fdef"),
        "url was: {}",
        calls[0].url
    );
}

#[test]
fn get_swap_defaults_id_to_the_requested_id_when_response_omits_it() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    transport.push_json(serde_json::json!({"capacity_sats": 1}));

    let c = client(&transport, &signer);
    let detail = c.get_swap("swap-9").unwrap();
    assert_eq!(detail.id, "swap-9");
}

// -------------------------------------------------------------- get_my_swaps

#[test]
fn get_my_swaps_normalizes_all_three_buckets() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "c");
    transport.push_json(serde_json::json!({
        "pending": [{"id": 1}],
        "opening": [{"id": "2", "outgoing_peer_pubkey": pubkey(2), "deadline": "2026-01-01T00:00:00Z"}],
        "completed": []
    }));

    let c = client(&transport, &signer);
    let my = c.get_my_swaps().unwrap();

    assert_eq!(my.pending.len(), 1);
    assert_eq!(my.pending[0].id, "1");
    assert_eq!(my.opening.len(), 1);
    assert_eq!(my.opening[0].outgoing_peer_pubkey, Some(pubkey(2)));
    assert!(my.completed.is_empty());
}

#[test]
fn get_my_swaps_empty_list_envelope_falls_back_to_empty_buckets() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "c");
    transport.push_json(serde_json::json!([]));

    let c = client(&transport, &signer);
    let my = c.get_my_swaps().unwrap();
    assert!(my.pending.is_empty() && my.opening.is_empty() && my.completed.is_empty());
}

#[test]
fn get_my_swaps_entries_without_id_are_dropped() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "c");
    transport.push_json(serde_json::json!({
        "pending": [{"capacity_sats": 1}, {"id": "keep-me"}],
        "opening": [],
        "completed": []
    }));

    let c = client(&transport, &signer);
    let my = c.get_my_swaps().unwrap();
    assert_eq!(my.pending.len(), 1);
    assert_eq!(my.pending[0].id, "keep-me");
}

// --------------------------------------------------- mutating endpoints

#[test]
fn create_application_attaches_id_and_succeeds_on_2xx() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "c");
    transport.push_json(serde_json::json!({"ok": true}));

    let c = client(&transport, &signer);
    assert!(c.create_application("swap-7").is_ok());

    let calls = transport.calls.borrow();
    let body = String::from_utf8(calls[1].body.clone().unwrap()).unwrap();
    assert!(body.contains("id=swap-7"));
}

#[test]
fn delete_application_smoke() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "c");
    transport.push_json(serde_json::json!({"ok": true}));
    let c = client(&transport, &signer);
    assert!(c.delete_application("swap-7").is_ok());
}

#[test]
fn complete_application_smoke() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "c");
    transport.push_json(serde_json::json!({"ok": true}));
    let c = client(&transport, &signer);
    assert!(c.complete_application("swap-7").is_ok());
}

#[test]
fn create_rating_sends_positive_or_negative_string() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "c");
    transport.push_json(serde_json::json!({"ok": true}));
    let c = client(&transport, &signer);
    assert!(c.create_rating("swap-7", Rating::Negative).is_ok());

    let calls = transport.calls.borrow();
    let body = String::from_utf8(calls[1].body.clone().unwrap()).unwrap();
    assert!(body.contains("rating=negative"));
}

#[test]
fn mark_read_notifications_smoke() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "c");
    transport.push_json(serde_json::json!({"ok": true}));
    let c = client(&transport, &signer);
    assert!(c.mark_read_notifications().is_ok());
}

// ------------------------------------------------------------- notifications

#[test]
fn get_notifications_extracts_array_and_renames_type_to_kind() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "c");
    transport.push_json(serde_json::json!({
        "notifications": [
            {"type": "swap_completed", "body": "hello", "created_at": "2026-01-01", "url": "https://x"}
        ]
    }));

    let c = client(&transport, &signer);
    let notes = c.get_notifications().unwrap();
    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0].kind.as_deref(), Some("swap_completed"));
    assert_eq!(notes[0].body.as_deref(), Some("hello"));
    assert_eq!(notes[0].url.as_deref(), Some("https://x"));
}

#[test]
fn get_notifications_missing_key_is_empty_not_an_error() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "c");
    transport.push_json(serde_json::json!({}));
    let c = client(&transport, &signer);
    assert_eq!(c.get_notifications().unwrap().len(), 0);
}

// ---------------------------------------------------------------- error paths

#[test]
fn structural_422_error_body_is_parsed_into_errors_map() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "c");
    transport.push_status(
        422,
        serde_json::json!({"errors": {"id": ["already applied"]}}),
    );

    let c = client(&transport, &signer);
    let err = c.create_application("swap-7").unwrap_err();
    assert!(err.structural_contains(422, "already"));
    assert!(!err.structural_contains(400, "already"));
}

#[test]
fn non_json_error_body_yields_no_errors_map_control() {
    // CONTROL for the test above: an error body that ISN'T a parseable
    // errors dict must not fabricate a structural match.
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "c");
    transport.push_raw_status(422, b"plain text failure, not json");

    let c = client(&transport, &signer);
    let err = c.create_application("swap-7").unwrap_err();
    assert!(!err.structural_contains(422, "failure"));
    assert_eq!(err.http_status, Some(422));
}

#[test]
fn response_too_large_is_rejected() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "c");
    let huge = vec![b'a'; MAX_RESPONSE_BYTES + 1];
    transport.push_raw_status(200, &huge);

    let c = client(&transport, &signer);
    let err = c.get_applicable_swaps().unwrap_err();
    assert!(err.message.contains("too large"));
}

#[test]
fn control_response_at_exactly_the_cap_is_not_too_large() {
    // CONTROL for the too-large test: the boundary itself must still be
    // accepted (as invalid JSON, since it's not valid JSON — but NOT
    // rejected for size).
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "c");
    let at_cap = vec![b' '; MAX_RESPONSE_BYTES];
    transport.push_raw_status(200, &at_cap);

    let c = client(&transport, &signer);
    let err = c.get_applicable_swaps().unwrap_err();
    assert!(
        !err.message.contains("too large"),
        "exactly-at-cap must not trip the size guard: {}",
        err.message
    );
}

#[test]
fn invalid_json_body_is_rejected() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    queue_challenge(&transport, "c");
    transport.push_raw_status(200, b"{not valid json");

    let c = client(&transport, &signer);
    let err = c.get_applicable_swaps().unwrap_err();
    assert!(err.message.contains("invalid JSON"));
}

#[test]
fn transport_failure_surfaces_as_unreachable_error() {
    let transport = FakeHttpTransport::new();
    let signer = FakeSigner::new();
    transport.push_transport_err("connection refused");

    let c = client(&transport, &signer);
    let err = c.get_applicable_swaps().unwrap_err();
    assert!(err.message.contains("unreachable"));
    assert!(err.message.contains("connection refused"));
}
