//! `LnPlusApiClient` — a production [`crate::ports::LnPlusApi`] implementation
//! over HTTPS, porting `LNPlusClient` (`lnplus_swaps.py:72-229`): the
//! `BASE_URL`/endpoint set, the `_request` transport wrapper (size cap,
//! error/JSON handling, `_unwrap_list_envelope`), and `_auth_params`'
//! signmessage-based auth flow (`get_message` -> gate 15
//! [`crate::validation::validate_challenge`] -> `signmessage` -> attach
//! `message`+`signature`).
//!
//! # Why there is no real wire transport in this crate
//!
//! `Cargo.lock` has no HTTP client crate anywhere in the workspace
//! dependency graph (checked: no `reqwest`/`ureq`/`hyper`/`curl`/etc.), and
//! this crate's whole design principle (`Cargo.toml`'s own doc comment) is
//! "no HTTP client, no SQLite connection, no CLN RPC socket" at the KERNEL
//! layer. The wiring task this file belongs to permits adding a real
//! dependency OR keeping the transport behind a small trait and documenting
//! what a maintainer should add — this file takes the second path:
//!
//! - [`HttpTransport`] is that trait: one blocking `request` method,
//!   `std::process::Command`-free, with no bytes-on-the-wire opinion beyond
//!   "give me a status code and a body".
//! - [`LnPlusApiClient<T, S>`] is generic over `T: HttpTransport` (plus
//!   `S: `[`Signer`], the `rpc.signmessage` equivalent) and implements
//!   [`crate::ports::LnPlusApi`] completely — URL building, form encoding, JSON
//!   parsing, the auth flow, `_unwrap_list_envelope`, and the C-4
//!   structured-422-error parse all live here and are fully exercised by
//!   `tests/http_client.rs` against an in-memory [`HttpTransport`] fake.
//! - **No concrete `HttpTransport` ships in this crate** — by design, so
//!   that "no test may make a live HTTP request" (the task's hard
//!   requirement) is true by construction, not by discipline. A maintainer
//!   wiring this crate into the plugin should add **`ureq`** (small,
//!   synchronous — matches this plugin's blocking RPC style better than an
//!   async client would; pull in its `rustls`+`gzip` feature set only, no
//!   `native-tls`) as a `revops-lnplus`-local dependency and implement
//!   [`HttpTransport`] for a thin wrapper struct in ~15 lines: build an
//!   `ureq::Agent`, match on [`HttpMethod`], set the header list, `.send`
//!   the optional body, and map the `ureq::Response`/`ureq::Error` shapes
//!   into [`HttpResponse`]/[`TransportError`]. See `REGISTER.md` for the
//!   exact snippet shape expected at the call site.
//!
//! Similarly, [`Signer`] has no concrete implementation here — a real one
//! wraps whatever CLN RPC client the plugin already uses (`revops-rpc`) and
//! calls its `signmessage` method, extracting the `zbase` field Python's
//! `signed.get("zbase")` reads (py 131).

use std::fmt;

use serde_json::Value;

use crate::error::{ErrorsMap, LnPlusError};
use crate::ports::LnPlusApi;
use crate::types::{
    MySwapEntry, MySwaps, NotificationEntry, Participant, Rating, SwapDetail, SwapListing,
};
use crate::validation::{validate_challenge, TsValue};

/// py 24 `BASE_URL`.
pub const BASE_URL: &str = "https://lightningnetwork.plus/api/2";
/// py 25 `_MAX_RESPONSE_BYTES`.
pub const MAX_RESPONSE_BYTES: usize = 1_000_000;

/// The only two HTTP methods `LNPlusClient` ever issues (py `_request`'s
/// `method` parameter).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
}

/// What a transport hands back: whatever status code and body bytes the
/// server sent, no interpretation. Non-2xx is NOT an error at this level —
/// [`LnPlusApiClient`] classifies it, matching py's
/// `urllib.error.HTTPError` vs. everything-else split.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// Task 61 4C: where in the exchange a transport failure happened — the
/// bit 4B's typed submission outcomes are built from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportPhase {
    /// Provably failed before the request bytes could have reached the
    /// server (DNS, connect refused, TLS handshake, invalid URL). A
    /// mutating call that fails here is KNOWN not-submitted.
    BeforeRequestSent,
    /// The request may have reached the server (timeout awaiting the
    /// response, reset mid-exchange, read error). A mutating call that
    /// fails here has an UNKNOWN outcome — quarantine, never treat as a
    /// clean failure. When in doubt a transport implementation MUST pick
    /// this phase (fail toward quarantine).
    AfterRequestSent,
}

/// A transport-level failure — DNS, TCP, TLS, timeout — with no HTTP
/// response at all (py's `urllib.error.URLError`/`OSError`/`TimeoutError`
/// branch, `_request` lines 116-117), carrying its [`TransportPhase`].
#[derive(Debug, Clone)]
pub struct TransportError {
    pub message: String,
    pub phase: TransportPhase,
}

impl TransportError {
    pub fn before_send(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            phase: TransportPhase::BeforeRequestSent,
        }
    }
    pub fn after_send(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            phase: TransportPhase::AfterRequestSent,
        }
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl std::error::Error for TransportError {}

/// The seam a real HTTP crate implements. See this module's doc comment —
/// no concrete implementation ships in this crate.
pub trait HttpTransport {
    /// `url` is already fully assembled (query string included for GET).
    /// `body`, when `Some`, is the exact bytes to send (already
    /// form-urlencoded by the caller) with `Content-Type:
    /// application/x-www-form-urlencoded` already present in `headers`.
    fn request(
        &self,
        method: HttpMethod,
        url: &str,
        headers: &[(String, String)],
        body: Option<Vec<u8>>,
    ) -> Result<HttpResponse, TransportError>;
}

/// Ergonomic seam: a shared reference to any [`HttpTransport`] is itself
/// one, so callers (and `tests/http_client.rs`) can construct
/// `LnPlusApiClient::new(&transport, &signer)` and keep their own handle on
/// `transport`/`signer` for post-call inspection instead of losing them by
/// moving into the client.
impl<X: HttpTransport + ?Sized> HttpTransport for &X {
    fn request(
        &self,
        method: HttpMethod,
        url: &str,
        headers: &[(String, String)],
        body: Option<Vec<u8>>,
    ) -> Result<HttpResponse, TransportError> {
        (**self).request(method, url, headers, body)
    }
}

/// A signmessage failure (py: `signed.get("zbase")` missing/falsy, or the
/// RPC call itself raising).
#[derive(Debug, Clone)]
pub struct SignError(pub String);

impl fmt::Display for SignError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for SignError {}

/// The `rpc.signmessage(message)` equivalent (py 130-133). A real
/// implementation calls CLN's `signmessage` RPC and extracts the `zbase`
/// field itself — this trait's return value IS that field, not the raw RPC
/// response, keeping the "unwrap the dict" responsibility out of
/// [`LnPlusApiClient`] (which has no CLN RPC response shape to know about).
pub trait Signer {
    fn signmessage(&self, message: &str) -> Result<String, SignError>;
}

/// See the analogous `impl HttpTransport for &X` above.
impl<X: Signer + ?Sized> Signer for &X {
    fn signmessage(&self, message: &str) -> Result<String, SignError> {
        (**self).signmessage(message)
    }
}

/// Production [`LnPlusApi`], generic over the transport and signer seams.
/// Holds no other state — every call is a fresh `get_message` + signed
/// round trip, exactly matching Python (no session/cookie reuse).
pub struct LnPlusApiClient<T, S> {
    transport: T,
    signer: S,
    base_url: String,
}

impl<T: HttpTransport, S: Signer> LnPlusApiClient<T, S> {
    pub fn new(transport: T, signer: S) -> Self {
        Self {
            transport,
            signer,
            base_url: BASE_URL.trim_end_matches('/').to_string(),
        }
    }

    /// Test/self-hosted-mirror seam (py's `base_url` constructor param).
    pub fn with_base_url(transport: T, signer: S, base_url: impl Into<String>) -> Self {
        Self {
            transport,
            signer,
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    // -- transport -----------------------------------------------------

    /// py `_request` (82-123).
    fn request(
        &self,
        path: &str,
        params: Option<&[(String, String)]>,
        method: HttpMethod,
    ) -> Result<Value, LnPlusError> {
        let mut url = format!("{}/{path}", self.base_url);
        let mut headers = vec![
            ("Accept".to_string(), "application/json".to_string()),
            ("User-Agent".to_string(), "cl-revenue-ops".to_string()),
        ];
        let body = match method {
            HttpMethod::Post => {
                headers.push((
                    "Content-Type".to_string(),
                    "application/x-www-form-urlencoded".to_string(),
                ));
                Some(urlencode_form(params.unwrap_or(&[])).into_bytes())
            }
            HttpMethod::Get => {
                if let Some(p) = params {
                    if !p.is_empty() {
                        url = format!("{url}?{}", urlencode_form(p));
                    }
                }
                None
            }
        };

        // Task 61 4C phase mapping (feeds 4B's typed submission outcomes):
        //  - transport failure BEFORE the request went out → clean/known;
        //  - transport failure AFTER it may have gone out → OUTCOME UNKNOWN;
        //  - a 2xx the client cannot interpret (over-cap, unparseable) →
        //    OUTCOME UNKNOWN (the server said yes; only our read failed);
        //  - a structured non-2xx → clean/known refusal.
        let resp = self
            .transport
            .request(method, &url, &headers, body)
            .map_err(|e| match e.phase {
                TransportPhase::BeforeRequestSent => {
                    LnPlusError::new(format!("LN+ unreachable on {path}: {e}"))
                }
                TransportPhase::AfterRequestSent => LnPlusError::unknown_outcome(format!(
                    "LN+ transport failed after the request may have been sent on {path}: {e}"
                )),
            })?;

        if !(200..300).contains(&resp.status) {
            let snippet = body_snippet(&resp.body);
            let parsed_errors = parse_errors_field(&resp.body);
            return Err(match parsed_errors {
                Some(errors) => LnPlusError::with_errors(
                    format!("LN+ HTTP {} on {path}: {snippet}", resp.status),
                    resp.status,
                    errors,
                ),
                None => LnPlusError::with_status(
                    format!("LN+ HTTP {} on {path}: {snippet}", resp.status),
                    resp.status,
                ),
            });
        }

        if resp.body.len() > MAX_RESPONSE_BYTES {
            return Err(LnPlusError::unknown_outcome(format!(
                "LN+ response too large on {path} — 2xx received but unreadable"
            )));
        }

        serde_json::from_slice(&resp.body).map_err(|e| {
            LnPlusError::unknown_outcome(format!(
                "LN+ invalid JSON on {path} — 2xx received but unreadable: {e}"
            ))
        })
    }

    // -- auth ------------------------------------------------------------

    /// py `_auth_params` (126-134) + `_validate_challenge` (gate 15,
    /// 136-148, ported to [`crate::validation::validate_challenge`]).
    ///
    /// Task 61 4C: any failure HERE — whatever its transport phase — is a
    /// clean/known failure for the caller's mutating request, which was
    /// never issued. The unknown flag is stripped so a challenge hiccup
    /// can never quarantine a mutation that did not happen.
    fn auth_params(&self) -> Result<Vec<(String, String)>, LnPlusError> {
        let challenge = self
            .request("get_message", None, HttpMethod::Get)
            .map_err(|mut e| {
                e.outcome_unknown = false;
                e
            })?;
        let message = challenge.get("message").and_then(Value::as_str);
        validate_challenge(message)?;
        // `validate_challenge` already rejected `None`/empty, so this is
        // known-`Some` — no `unwrap_or_default` masking a real bug here.
        let message = message
            .expect("validate_challenge accepted a None message")
            .to_string();
        let signature = self
            .signer
            .signmessage(&message)
            .map_err(|e| LnPlusError::new(format!("signmessage failed: {e}")))?;
        if signature.is_empty() {
            return Err(LnPlusError::new("signmessage returned no zbase signature"));
        }
        Ok(vec![
            ("message".to_string(), message),
            ("signature".to_string(), signature),
        ])
    }
}

impl<T: HttpTransport, S: Signer> LnPlusApi for LnPlusApiClient<T, S> {
    /// py `get_applicable_swaps` (151-159).
    fn get_applicable_swaps(&self) -> Result<Vec<SwapListing>, LnPlusError> {
        let params = self.auth_params()?;
        let result = self.request("get_applicable_swaps", Some(&params), HttpMethod::Post)?;
        let swaps = result
            .as_object()
            .and_then(|o| o.get("swaps"))
            .cloned()
            .unwrap_or(result);
        let arr = swaps.as_array().cloned().unwrap_or_default();
        Ok(arr.iter().map(parse_swap_listing).collect())
    }

    /// py `get_swap` (161-167).
    fn get_swap(&self, swap_id: &str) -> Result<SwapDetail, LnPlusError> {
        let path = format!("get_swap/id={}", percent_encode_path_segment(swap_id));
        let result = self.request(&path, None, HttpMethod::Get)?;
        let result = unwrap_list_envelope(result, Value::Object(Default::default()));
        Ok(parse_swap_detail(&result, swap_id))
    }

    /// py `get_my_swaps` (169-181).
    fn get_my_swaps(&self) -> Result<MySwaps, LnPlusError> {
        let params = self.auth_params()?;
        let result = self.request("get_my_swaps", Some(&params), HttpMethod::Post)?;
        let fallback = serde_json::json!({"pending": [], "opening": [], "completed": []});
        let result = unwrap_list_envelope(result, fallback);
        let obj = result
            .as_object()
            .ok_or_else(|| LnPlusError::new("get_my_swaps: unexpected payload"))?;
        Ok(MySwaps {
            pending: parse_my_swap_entries(obj.get("pending")),
            opening: parse_my_swap_entries(obj.get("opening")),
            completed: parse_my_swap_entries(obj.get("completed")),
        })
    }

    /// py `create_application` (197-200).
    fn create_application(&self, swap_id: &str) -> Result<(), LnPlusError> {
        let mut params = self.auth_params()?;
        params.push(("id".to_string(), swap_id.to_string()));
        self.request("create_application", Some(&params), HttpMethod::Post)?;
        Ok(())
    }

    /// py `delete_application` (202-205).
    fn delete_application(&self, swap_id: &str) -> Result<(), LnPlusError> {
        let mut params = self.auth_params()?;
        params.push(("id".to_string(), swap_id.to_string()));
        self.request("delete_application", Some(&params), HttpMethod::Post)?;
        Ok(())
    }

    /// py `complete_application` (207-210).
    fn complete_application(&self, swap_id: &str) -> Result<(), LnPlusError> {
        let mut params = self.auth_params()?;
        params.push(("id".to_string(), swap_id.to_string()));
        self.request("complete_application", Some(&params), HttpMethod::Post)?;
        Ok(())
    }

    /// py `get_notifications` (212-217) + the array-extraction Python does
    /// at the `_poll_notifications` call site (2096) — pulled in here
    /// because [`LnPlusApi::get_notifications`]'s signature already commits
    /// to returning `Vec<NotificationEntry>`, not a raw envelope.
    fn get_notifications(&self) -> Result<Vec<NotificationEntry>, LnPlusError> {
        let params = self.auth_params()?;
        let result = self.request("get_notifications", Some(&params), HttpMethod::Post)?;
        let result = unwrap_list_envelope(result, Value::Object(Default::default()));
        let obj = result
            .as_object()
            .ok_or_else(|| LnPlusError::new("get_notifications: unexpected payload"))?;
        let notes = obj
            .get("notifications")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        Ok(notes.iter().map(parse_notification).collect())
    }

    /// py `mark_read_notifications` (219-221).
    fn mark_read_notifications(&self) -> Result<(), LnPlusError> {
        let params = self.auth_params()?;
        self.request("mark_read_notifications", Some(&params), HttpMethod::Post)?;
        Ok(())
    }

    /// py `create_rating` (223-229). Rust's [`Rating`] enum already makes
    /// an invalid rating value unrepresentable, so the `rating not in
    /// (...)` guard (py 224-225) has no Rust equivalent to port — there is
    /// no third variant to reject.
    fn create_rating(&self, swap_id: &str, rating: Rating) -> Result<(), LnPlusError> {
        let mut params = self.auth_params()?;
        params.push(("id".to_string(), swap_id.to_string()));
        params.push(("rating".to_string(), rating.as_str().to_string()));
        self.request("create_rating", Some(&params), HttpMethod::Post)?;
        Ok(())
    }
}

// -- JSON parsing helpers ---------------------------------------------------

/// py `_unwrap_list_envelope` (183-195).
fn unwrap_list_envelope(result: Value, empty_fallback: Value) -> Value {
    match result {
        Value::Array(mut arr) => {
            if arr.is_empty() {
                empty_fallback
            } else {
                let first = arr.remove(0);
                if first.is_object() {
                    first
                } else {
                    Value::Array(arr)
                }
            }
        }
        other => other,
    }
}

fn body_snippet(body: &[u8]) -> String {
    let truncated = &body[..body.len().min(500)];
    String::from_utf8_lossy(truncated).to_string()
}

/// C-4 (py 101-113): a best-effort parse of a non-2xx body's `"errors"`
/// key, when present and a dict-of-string-or-list-of-strings shape.
fn parse_errors_field(body: &[u8]) -> Option<ErrorsMap> {
    let v: Value = serde_json::from_slice(body).ok()?;
    let obj = v.as_object()?;
    let errors = obj.get("errors")?.as_object()?;
    let mut map = ErrorsMap::new();
    for (k, val) in errors {
        let messages: Vec<String> = match val {
            Value::String(s) => vec![s.clone()],
            Value::Array(arr) => arr
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect(),
            _ => continue,
        };
        map.insert(k.clone(), messages);
    }
    Some(map)
}

fn as_i64_lenient(v: Option<&Value>) -> i64 {
    v.and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
        .unwrap_or(0)
}

fn as_str_owned(v: Option<&Value>) -> Option<String> {
    v.and_then(Value::as_str).map(str::to_string)
}

fn as_bool_lenient(v: Option<&Value>) -> bool {
    v.and_then(Value::as_bool).unwrap_or(false)
}

/// LN+ ids are documented as sometimes-numeric on the wire (py 156-158,
/// 165-166, 179-180 all normalize `swap["id"] = str(swap["id"])`).
fn as_id_string(v: Option<&Value>) -> String {
    match v {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        _ => String::new(),
    }
}

fn parse_participant(v: &Value) -> Participant {
    Participant {
        participant_identifier: as_str_owned(v.get("participant_identifier")),
        pubkey: as_str_owned(v.get("pubkey")),
        cancelled: as_bool_lenient(v.get("cancelled")),
        banned: as_bool_lenient(v.get("banned")),
        address_1: as_str_owned(v.get("address_1")),
        address_2: as_str_owned(v.get("address_2")),
        positive_ratings_count: as_i64_lenient(v.get("positive_ratings_count")),
        negative_ratings_count: as_i64_lenient(v.get("negative_ratings_count")),
        lnplus_rank_number: as_i64_lenient(v.get("lnplus_rank_number")),
    }
}

fn parse_participants(v: &Value) -> Vec<Participant> {
    v.get("participants")
        .and_then(Value::as_array)
        .map(|arr| arr.iter().map(parse_participant).collect())
        .unwrap_or_default()
}

fn parse_swap_listing(v: &Value) -> SwapListing {
    SwapListing {
        id: as_id_string(v.get("id")),
        status: as_str_owned(v.get("status")).unwrap_or_default(),
        participant_waiting_for_count: as_i64_lenient(v.get("participant_waiting_for_count")),
        capacity_sats: as_i64_lenient(v.get("capacity_sats")),
        duration_months: as_i64_lenient(v.get("duration_months")),
        participant_max_count: as_i64_lenient(v.get("participant_max_count")),
        platform: as_str_owned(v.get("platform")),
        participants: parse_participants(v),
    }
}

/// `fallback_id`: used when the response omits `"id"` (SwapDetail has no
/// `Option<String>` id field to leave empty) — the swap id the CALLER
/// requested is the only sane default, not a Python behavior being ported.
fn parse_swap_detail(v: &Value, fallback_id: &str) -> SwapDetail {
    let id = as_id_string(v.get("id"));
    SwapDetail {
        id: if id.is_empty() {
            fallback_id.to_string()
        } else {
            id
        },
        capacity_sats: v.get("capacity_sats").and_then(Value::as_i64),
        duration_months: v.get("duration_months").and_then(Value::as_i64),
        participants: parse_participants(v),
    }
}

fn parse_ts_value(v: Option<&Value>) -> Option<TsValue> {
    match v {
        Some(Value::String(s)) => Some(TsValue::Iso(s.clone())),
        Some(Value::Number(n)) => n.as_f64().map(TsValue::Epoch),
        _ => None,
    }
}

fn parse_my_swap_entry(v: &Value) -> MySwapEntry {
    MySwapEntry {
        id: as_id_string(v.get("id")),
        capacity_sats: v.get("capacity_sats").and_then(Value::as_i64),
        duration_months: v.get("duration_months").and_then(Value::as_i64),
        outgoing_peer_pubkey: as_str_owned(v.get("outgoing_peer_pubkey")),
        outgoing_peer_clearnet_address: as_str_owned(v.get("outgoing_peer_clearnet_address")),
        outgoing_peer_tor_address: as_str_owned(v.get("outgoing_peer_tor_address")),
        incoming_peer_pubkey: as_str_owned(v.get("incoming_peer_pubkey")),
        deadline: parse_ts_value(v.get("deadline")),
        ends: parse_ts_value(v.get("ends")),
    }
}

fn parse_my_swap_entries(v: Option<&Value>) -> Vec<MySwapEntry> {
    v.and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter(|e| e.get("id").is_some())
                .map(parse_my_swap_entry)
                .collect()
        })
        .unwrap_or_default()
}

/// LN+'s wire field is `"type"` (py 2101, 2103) — renamed to `kind` in
/// [`NotificationEntry`] since `type` is a Rust keyword.
fn parse_notification(v: &Value) -> NotificationEntry {
    NotificationEntry {
        kind: as_str_owned(v.get("type")),
        body: as_str_owned(v.get("body")),
        created_at: as_str_owned(v.get("created_at")),
        url: as_str_owned(v.get("url")),
    }
}

// -- encoding ----------------------------------------------------------

/// `application/x-www-form-urlencoded`, matching
/// `urllib.parse.urlencode`'s default (`quote_via=quote_plus`): unreserved
/// chars pass through, space -> `+`, everything else `%XX` (uppercase hex).
/// Used for both the POST body and a GET query string (py `_request` uses
/// the same `urlencode` call for both, 87 and 90).
fn urlencode_form(pairs: &[(String, String)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", quote_plus(k), quote_plus(v)))
        .collect::<Vec<_>>()
        .join("&")
}

fn quote_plus(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// `urllib.parse.quote(str(swap_id), safe='')` (py 162): full percent
/// encoding, including `/`.
fn percent_encode_path_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
