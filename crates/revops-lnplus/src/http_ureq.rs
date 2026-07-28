//! Task 61 4C — the CONCRETE production [`HttpTransport`]: a thin `ureq`
//! (rustls) wrapper. This is the one place transport failures acquire
//! their [`TransportPhase`], feeding 4B's typed submission outcomes.
//!
//! Phase classification is deliberately CONSERVATIVE: only failures that
//! provably happen before the request bytes could have reached the server
//! (DNS, TCP connect, invalid URL/scheme, proxy setup, HTTPS policy) are
//! `BeforeRequestSent`. Everything else — timeouts, resets, read errors,
//! protocol garbage — is `AfterRequestSent`, because once ureq starts the
//! exchange we cannot prove the request did NOT arrive; the kernel then
//! quarantines rather than risking a duplicate irreversible submit.
//!
//! No test anywhere touches the network beyond loopback: this type is
//! exercised only against a local TCP fake (`tests/ureq_transport.rs`).
//! The TLS stack (rustls via ureq's default feature) is exercised in
//! production only; local tests use plain `http://127.0.0.1` URLs.

use std::io::Read;
use std::time::Duration;

use crate::http::{HttpMethod, HttpResponse, HttpTransport, TransportError, MAX_RESPONSE_BYTES};

/// Default per-request timeout, matching Python's `urlopen(timeout=20)`
/// (`lnplus_swaps.py:96`).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(20);

/// Production [`HttpTransport`] over one pooled `ureq::Agent`.
pub struct UreqTransport {
    agent: ureq::Agent,
}

impl UreqTransport {
    pub fn new() -> Self {
        Self::with_timeout(DEFAULT_TIMEOUT)
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(timeout)
            .timeout(timeout)
            // No redirect following: a redirected POST re-submit would be
            // a second wire write of an irreversible call.
            .redirects(0)
            .build();
        Self { agent }
    }
}

impl Default for UreqTransport {
    fn default() -> Self {
        Self::new()
    }
}

/// See the module doc: pre-send failures must be PROVABLY pre-send;
/// everything else fails toward `AfterRequestSent` (quarantine).
fn is_before_send(kind: ureq::ErrorKind) -> bool {
    matches!(
        kind,
        ureq::ErrorKind::InvalidUrl
            | ureq::ErrorKind::UnknownScheme
            | ureq::ErrorKind::Dns
            | ureq::ErrorKind::ConnectionFailed
            | ureq::ErrorKind::InsecureRequestHttpsOnly
            | ureq::ErrorKind::InvalidProxyUrl
            | ureq::ErrorKind::ProxyConnect
            | ureq::ErrorKind::ProxyUnauthorized
    )
}

impl HttpTransport for UreqTransport {
    fn request(
        &self,
        method: HttpMethod,
        url: &str,
        headers: &[(String, String)],
        body: Option<Vec<u8>>,
    ) -> Result<HttpResponse, TransportError> {
        let verb = match method {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
        };
        let mut req = self.agent.request(verb, url);
        for (name, value) in headers {
            req = req.set(name, value);
        }
        let result = match body {
            Some(bytes) => req.send_bytes(&bytes),
            None => req.call(),
        };
        let resp = match result {
            Ok(resp) => resp,
            // ureq surfaces non-2xx as Error::Status — at THIS layer a
            // received response is a response, never a transport error
            // (the client classifies statuses).
            Err(ureq::Error::Status(_code, resp)) => resp,
            Err(ureq::Error::Transport(t)) => {
                let msg = format!("{t}");
                return Err(if is_before_send(t.kind()) {
                    TransportError::before_send(msg)
                } else {
                    TransportError::after_send(msg)
                });
            }
        };

        let status = resp.status();
        // Bounded read: at most MAX+1 bytes ever buffered, so an
        // oversized body trips the client's cap check without unbounded
        // memory. A body READ failure is after-send by definition.
        let mut body_bytes = Vec::new();
        resp.into_reader()
            .take(MAX_RESPONSE_BYTES as u64 + 1)
            .read_to_end(&mut body_bytes)
            .map_err(|e| TransportError::after_send(format!("response body read failed: {e}")))?;
        Ok(HttpResponse {
            status,
            body: body_bytes,
        })
    }
}
