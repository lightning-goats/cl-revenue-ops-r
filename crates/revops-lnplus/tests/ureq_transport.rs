//! Task 61 4C — the concrete `UreqTransport` against a LOCAL TCP fake
//! server (loopback only; no test here ever leaves 127.0.0.1). Covers the
//! contract cases: exact request on the wire, roundtrip, non-2xx
//! passthrough, connect-refused = before-send, reset/timeout after the
//! request = after-send, bounded oversized-body read, single-attempt
//! (no internal retry/resubmit), and recovery on the same transport.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use revops_lnplus::http::{HttpMethod, HttpTransport, TransportPhase, MAX_RESPONSE_BYTES};
use revops_lnplus::http_ureq::UreqTransport;

/// What the fake does with each accepted connection, in order.
enum Behavior {
    /// Read the full request, answer with the given status line + body.
    Respond { status: u16, body: Vec<u8> },
    /// Read the full request, then close without any response bytes.
    CloseAfterRequest,
    /// Read the full request, then sleep past the client timeout.
    StallAfterRequest(Duration),
}

struct FakeHttpServer {
    url_base: String,
    requests: mpsc::Receiver<String>,
    handle: thread::JoinHandle<usize>,
}

impl FakeHttpServer {
    /// Serves the scripted behaviors, one connection each, then exits.
    /// The join handle returns how many connections were ACCEPTED — the
    /// no-internal-retry assertion reads it.
    fn spawn(behaviors: Vec<Behavior>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let url_base = format!("http://{}", listener.local_addr().unwrap());
        let (req_tx, requests) = mpsc::channel();
        let handle = thread::spawn(move || {
            let mut accepted = 0usize;
            for behavior in behaviors {
                let (mut stream, _) = match listener.accept() {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                accepted += 1;
                let request = read_http_request(&mut stream);
                let _ = req_tx.send(request);
                match behavior {
                    Behavior::Respond { status, body } => {
                        let head = format!(
                            "HTTP/1.1 {status} X\r\ncontent-length: {}\r\n\
                             content-type: application/json\r\nconnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = stream.write_all(head.as_bytes());
                        let _ = stream.write_all(&body);
                    }
                    Behavior::CloseAfterRequest => drop(stream),
                    Behavior::StallAfterRequest(d) => {
                        thread::sleep(d);
                        drop(stream);
                    }
                }
            }
            accepted
        });
        Self {
            url_base,
            requests,
            handle,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{path}", self.url_base)
    }

    fn next_request(&self) -> String {
        self.requests
            .recv_timeout(Duration::from_secs(5))
            .expect("request recorded")
    }

    fn accepted_connections(self) -> usize {
        self.handle.join().expect("server thread")
    }
}

/// Minimal HTTP/1.1 request reader: headers to CRLFCRLF, then
/// content-length body bytes.
fn read_http_request(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(1) => buf.push(byte[0]),
            _ => break,
        }
    }
    let head = String::from_utf8_lossy(&buf).to_string();
    let content_length: usize = head
        .lines()
        .find_map(|l| {
            let (name, value) = l.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().ok())?
        })
        .unwrap_or(0);
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        let _ = stream.read_exact(&mut body);
    }
    format!("{head}{}", String::from_utf8_lossy(&body))
}

fn form_headers() -> Vec<(String, String)> {
    vec![
        ("Accept".to_string(), "application/json".to_string()),
        ("User-Agent".to_string(), "cl-revenue-ops".to_string()),
        (
            "Content-Type".to_string(),
            "application/x-www-form-urlencoded".to_string(),
        ),
    ]
}

#[test]
fn post_sends_the_exact_request_on_the_wire() {
    let server = FakeHttpServer::spawn(vec![Behavior::Respond {
        status: 200,
        body: b"{}".to_vec(),
    }]);
    let t = UreqTransport::with_timeout(Duration::from_secs(5));
    let resp = t
        .request(
            HttpMethod::Post,
            &server.url("api/2/create_application"),
            &form_headers(),
            Some(b"message=login+123&signature=zbase&id=s1".to_vec()),
        )
        .expect("roundtrip");
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, b"{}");

    let request = server.next_request();
    assert!(
        request.starts_with("POST /api/2/create_application HTTP/1.1\r\n"),
        "wire: {request:?}"
    );
    let lower = request.to_ascii_lowercase();
    assert!(lower.contains("content-type: application/x-www-form-urlencoded"));
    assert!(lower.contains("user-agent: cl-revenue-ops"));
    assert!(request.ends_with("message=login+123&signature=zbase&id=s1"));
}

#[test]
fn get_sends_query_string_and_no_body() {
    let server = FakeHttpServer::spawn(vec![Behavior::Respond {
        status: 200,
        body: b"{\"message\":\"m\"}".to_vec(),
    }]);
    let t = UreqTransport::with_timeout(Duration::from_secs(5));
    t.request(
        HttpMethod::Get,
        &server.url("api/2/get_swap/id%3D42?x=1"),
        &[("Accept".to_string(), "application/json".to_string())],
        None,
    )
    .expect("roundtrip");
    let request = server.next_request();
    assert!(
        request.starts_with("GET /api/2/get_swap/id%3D42?x=1 HTTP/1.1\r\n"),
        "wire: {request:?}"
    );
}

#[test]
fn non_2xx_response_is_a_response_not_a_transport_error() {
    let server = FakeHttpServer::spawn(vec![Behavior::Respond {
        status: 422,
        body: b"{\"errors\":{\"id\":[\"already applied\"]}}".to_vec(),
    }]);
    let t = UreqTransport::with_timeout(Duration::from_secs(5));
    let resp = t
        .request(
            HttpMethod::Post,
            &server.url("api/2/create_application"),
            &form_headers(),
            Some(b"id=s1".to_vec()),
        )
        .expect("a 422 is a received response at the transport layer");
    assert_eq!(resp.status, 422);
    assert!(String::from_utf8_lossy(&resp.body).contains("already applied"));
}

#[test]
fn connection_refused_is_before_send() {
    // Bind then drop: the port is (very likely) unbound for the request.
    let port = {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let t = UreqTransport::with_timeout(Duration::from_secs(2));
    let err = t
        .request(
            HttpMethod::Post,
            &format!("http://127.0.0.1:{port}/api/2/create_application"),
            &form_headers(),
            Some(b"id=s1".to_vec()),
        )
        .unwrap_err();
    assert_eq!(
        err.phase,
        TransportPhase::BeforeRequestSent,
        "connect refused is provably pre-send: {err}"
    );
}

#[test]
fn reset_after_the_request_is_after_send() {
    let server = FakeHttpServer::spawn(vec![Behavior::CloseAfterRequest]);
    let t = UreqTransport::with_timeout(Duration::from_secs(5));
    let err = t
        .request(
            HttpMethod::Post,
            &server.url("api/2/create_application"),
            &form_headers(),
            Some(b"id=s1".to_vec()),
        )
        .unwrap_err();
    assert_eq!(
        err.phase,
        TransportPhase::AfterRequestSent,
        "the request reached the server before the reset: {err}"
    );
    // The server genuinely read the request first.
    assert!(server.next_request().starts_with("POST "));
}

#[test]
fn timeout_awaiting_the_response_is_after_send_with_no_retry() {
    let server = FakeHttpServer::spawn(vec![Behavior::StallAfterRequest(Duration::from_millis(
        1500,
    ))]);
    let t = UreqTransport::with_timeout(Duration::from_millis(300));
    let err = t
        .request(
            HttpMethod::Post,
            &server.url("api/2/create_application"),
            &form_headers(),
            Some(b"id=s1".to_vec()),
        )
        .unwrap_err();
    assert_eq!(err.phase, TransportPhase::AfterRequestSent, "{err}");
    assert_eq!(
        server.accepted_connections(),
        1,
        "the transport must NOT retry an irreversible submit on its own"
    );
}

#[test]
fn oversized_body_read_is_bounded_at_cap_plus_one() {
    let server = FakeHttpServer::spawn(vec![Behavior::Respond {
        status: 200,
        body: vec![b'x'; MAX_RESPONSE_BYTES + 4096],
    }]);
    let t = UreqTransport::with_timeout(Duration::from_secs(10));
    let resp = t
        .request(
            HttpMethod::Get,
            &server.url("api/2/get_applicable_swaps"),
            &[("Accept".to_string(), "application/json".to_string())],
            None,
        )
        .expect("bounded read still yields a response");
    assert_eq!(
        resp.body.len(),
        MAX_RESPONSE_BYTES + 1,
        "read must stop at cap+1 (enough for the client to detect over-cap, no more)"
    );
}

#[test]
fn transport_recovers_on_the_next_request_after_a_failure() {
    let server = FakeHttpServer::spawn(vec![
        Behavior::CloseAfterRequest,
        Behavior::Respond {
            status: 200,
            body: b"{}".to_vec(),
        },
    ]);
    let t = UreqTransport::with_timeout(Duration::from_secs(5));
    let first = t.request(
        HttpMethod::Post,
        &server.url("api/2/create_application"),
        &form_headers(),
        Some(b"id=s1".to_vec()),
    );
    assert!(first.is_err());
    let second = t
        .request(
            HttpMethod::Post,
            &server.url("api/2/create_application"),
            &form_headers(),
            Some(b"id=s2".to_vec()),
        )
        .expect("same transport recovers");
    assert_eq!(second.status, 200);
    assert_eq!(server.accepted_connections(), 2);
}
