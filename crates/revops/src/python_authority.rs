//! Read-only Python-authority client + status validation (stateful-shadow
//! revision, Task 8 steps 2-3).
//!
//! This is the gate the live-fee-authority path checks Python actually
//! believes it has handed off: an `Ok(PythonAuthorityOff)` is proof that
//! the LAST thing Python's own status endpoint reported was "disabled, and
//! recently enough to trust." Anything else -- a missing/malformed/stale
//! response, `enabled=true`, or the RPC method not existing at all -- is a
//! denial. There is no "assume off" default anywhere in this module.
//!
//! ## `revenue-fee-authority-status` may not exist yet
//!
//! The Python-side RPC this client calls is part of a companion handoff
//! plan and may not be registered on the node yet. Per this task's ruling,
//! [`validate_status`] and [`validate_stable_epoch`] are pure functions
//! over already-fetched `serde_json::Value` responses -- every test in
//! `tests/python_authority.rs` exercises them against injected fixtures,
//! never a live socket. [`PythonAuthorityClient`] exists so a later task
//! (the live batch authorizer) has somewhere to call from, but nothing in
//! THIS task's verification requires the RPC to actually answer.
//!
//! ## Structurally read-only
//!
//! [`PythonAuthorityClient`] has exactly one method that ever touches the
//! `lightning-rpc` socket -- [`PythonAuthorityClient::fetch_raw_status`] --
//! and it calls a single hardcoded method name, [`AUTHORITY_STATUS_METHOD`],
//! through the SAME `revops_rpc::call_with_timeout` wrapper every other
//! read-only RPC surface in this crate uses (`fee_evidence::prefetch_rpc`,
//! `hydration::call_listforwards`). There is no method on this type that
//! accepts a caller-supplied method name, and no `setconfig` (or any other
//! action RPC) anywhere in this file -- the client cannot be used to
//! mutate anything, only to read one status.
//!
//! ## Two-read batch bracketing
//!
//! Per this task's ruling on "stable transition epoch across the batch
//! acquisition" (brief Step 2): the live batch authorizer is expected to
//! read status ONCE before assembling a batch and ONCE again immediately
//! before dispatch (a "fresh token"), then call [`validate_stable_epoch`]
//! on the two [`PythonAuthorityOff`] results. A `generation` or
//! `transitioned_at` change between the two reads means Python's authority
//! state moved during the batch window and the batch must be denied --
//! `observed_at` is deliberately excluded from the comparison (a fresh
//! read naturally has a later `observed_at`; only the EPOCH identity must
//! hold still).

use std::path::PathBuf;

use serde_json::Value;

use revops_rpc::RpcProxyError;

/// The ONLY RPC method [`PythonAuthorityClient`] can ever call. Kept as a
/// named constant (rather than inlined at the call site) so a future
/// second method cannot silently be added by editing a string literal --
/// widening this client's surface requires visibly touching this line.
pub const AUTHORITY_STATUS_METHOD: &str = "revenue-fee-authority-status";

/// A validated, parsed "Python authority is off" reading: the fields a
/// caller needs to (a) trust the reading (it already passed every check in
/// [`validate_status`]) and (b) bracket it against a second reading via
/// [`validate_stable_epoch`].
///
/// Deliberately NOT `Copy` (fix-round I1): a batch authorizer that wants to
/// bracket a batch acquisition must hold two DISTINCT values obtained from
/// two separate fetches -- `Copy` would let a call site silently duplicate
/// one reading (`let x = reading; validate_stable_epoch(&x, &x)`) without
/// the type system so much as raising an eyebrow. `Clone` is kept for
/// legitimate cases (e.g. logging a reading after it's been consumed);
/// [`validate_stable_epoch`] itself denies a cloned/duplicated reading via
/// its `observed_at` bracketing check regardless.
/// Task 59 F3: fields are PRIVATE -- the only constructor is
/// [`validate_status`], so a value of this type IS proof a raw response
/// passed every check. Forgery is a compile error:
///
/// ```compile_fail,E0451
/// let forged = revops::python_authority::PythonAuthorityOff {
///     generation: 0,
///     transitioned_at: 0,
///     observed_at: 0,
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PythonAuthorityOff {
    generation: u64,
    transitioned_at: i64,
    observed_at: i64,
}

impl PythonAuthorityOff {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn transitioned_at(&self) -> i64 {
        self.transitioned_at
    }

    pub fn observed_at(&self) -> i64 {
        self.observed_at
    }
}

/// Every fail-closed reason this module can return. Each is stable and
/// mutually exclusive; never reword an existing variant's message or
/// `code()` -- add a new variant instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PythonAuthorityDenyReason {
    /// The RPC transport failed for a reason other than a timeout or a
    /// method-not-found response (connection refused, socket missing,
    /// malformed JSON-RPC framing, ...). Carries the underlying message.
    Transport(String),
    /// The RPC call did not complete within its timeout budget.
    Timeout { seconds: u64 },
    /// The RPC error looks like lightningd's "no such command" response
    /// (JSON-RPC code `-32601`, or the `Unknown command` message CLN
    /// emits for an unregistered plugin RPC method) -- i.e. the Python
    /// plugin hasn't registered `revenue-fee-authority-status` at all.
    /// Distinguished from [`Self::Transport`] because "the handoff RPC
    /// doesn't exist on this node" is an operator-actionable, distinct
    /// fact from "the socket call failed".
    MethodNotFound,
    /// The response is not a JSON object at all.
    MalformedResponse(String),
    /// A required field is absent from the response object. Carries the
    /// field name.
    MissingField(&'static str),
    /// A required field is present but not the exact JSON type expected
    /// (`enabled` must be a JSON bool, the rest must be JSON integers --
    /// no string-to-bool/string-to-int coercion). Carries the field name.
    WrongFieldType(&'static str),
    /// `generation`, `transitioned_at`, or `observed_at` parsed as a
    /// negative integer. Carries the field name.
    NegativeField(&'static str),
    /// `enabled` was `true` -- Python still holds fee authority.
    StillEnabled,
    /// `now - observed_at` fell outside `[0, max_age_seconds]` -- either
    /// too old (a stale cached reading) or in the future (clock skew /
    /// corrupted timestamp). Carries the actual (possibly negative) age
    /// and the bound that was checked against.
    StaleObservation {
        age_seconds: i64,
        max_age_seconds: i64,
    },
    /// Two status reads bracketing a batch acquisition disagreed on
    /// `generation` and/or `transitioned_at` -- Python's authority state
    /// moved during the batch window.
    UnstableEpoch {
        first: (u64, i64),
        second: (u64, i64),
    },
    /// The second reading's `observed_at` did not strictly advance past the
    /// first's (fix-round I1): equal (including the same reading checked
    /// against itself) or earlier. "Bracketing" means the second read is a
    /// genuinely later fetch -- a non-advancing `observed_at` means no real
    /// second fetch happened (or Python's own clock went backward), and
    /// either way the batch must be denied.
    NonAdvancingObservation {
        first_observed_at: i64,
        second_observed_at: i64,
    },
    /// Task 59 R2-F2 (new variant -- existing codes never reworded): an
    /// OPEN bracket went stale before its close -- either its monotonic
    /// lifetime exceeded [`OPEN_BRACKET_MAX_LIFETIME`], or the FIRST
    /// reading's wall-clock age at authorization time exceeded the same
    /// `max_age_seconds` bound `validate_status` enforced at open. The
    /// second fetch is SKIPPED: a refused stale bracket performs exactly
    /// one total fetch.
    StaleOpenBracket { age_seconds: i64, max_seconds: i64 },
}

impl PythonAuthorityDenyReason {
    /// Stable, machine-matchable code -- logged and reported verbatim.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Transport(_) => "python_authority_transport_error",
            Self::Timeout { .. } => "python_authority_timeout",
            Self::MethodNotFound => "python_authority_method_not_found",
            Self::MalformedResponse(_) => "python_authority_malformed_response",
            Self::MissingField(_) => "python_authority_missing_field",
            Self::WrongFieldType(_) => "python_authority_wrong_field_type",
            Self::NegativeField(_) => "python_authority_negative_field",
            Self::StillEnabled => "python_authority_still_enabled",
            Self::StaleObservation { .. } => "python_authority_stale_observation",
            Self::UnstableEpoch { .. } => "python_authority_unstable_epoch",
            Self::NonAdvancingObservation { .. } => "python_authority_non_advancing_observation",
            Self::StaleOpenBracket { .. } => "python_authority_stale_open_bracket",
        }
    }
}

impl std::fmt::Display for PythonAuthorityDenyReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(detail) => write!(f, "{}: {detail}", self.code()),
            Self::Timeout { seconds } => {
                write!(f, "{}: no response within {seconds}s", self.code())
            }
            Self::MethodNotFound => write!(
                f,
                "{}: {AUTHORITY_STATUS_METHOD} is not a registered RPC method on this node",
                self.code(),
            ),
            Self::MalformedResponse(detail) => write!(f, "{}: {detail}", self.code()),
            Self::MissingField(field) => write!(f, "{}: missing field '{field}'", self.code()),
            Self::WrongFieldType(field) => {
                write!(
                    f,
                    "{}: field '{field}' has the wrong JSON type",
                    self.code()
                )
            }
            Self::NegativeField(field) => {
                write!(f, "{}: field '{field}' is negative", self.code())
            }
            Self::StillEnabled => write!(
                f,
                "{}: Python authority status reports enabled=true",
                self.code(),
            ),
            Self::StaleObservation {
                age_seconds,
                max_age_seconds,
            } => write!(
                f,
                "{}: observation age {age_seconds}s outside the bound [0, {max_age_seconds}]s",
                self.code(),
            ),
            Self::UnstableEpoch { first, second } => write!(
                f,
                "{}: generation/transitioned_at changed across the batch acquisition \
                 ({first:?} -> {second:?})",
                self.code(),
            ),
            Self::NonAdvancingObservation {
                first_observed_at,
                second_observed_at,
            } => write!(
                f,
                "{}: second read's observed_at ({second_observed_at}) did not strictly advance \
                 past the first's ({first_observed_at}) -- bracketing requires a genuinely \
                 later second fetch",
                self.code(),
            ),
            Self::StaleOpenBracket {
                age_seconds,
                max_seconds,
            } => write!(
                f,
                "{}: open bracket age {age_seconds}s exceeds the {max_seconds}s bound -- \
                 refused before the second fetch; open a fresh bracket",
                self.code(),
            ),
        }
    }
}

impl std::error::Error for PythonAuthorityDenyReason {}

/// Classify an [`RpcProxyError`] from the shared timeout wrapper into a
/// [`PythonAuthorityDenyReason`]. Pure and offline-testable: no socket is
/// touched here, only string matching over the error the wrapper already
/// produced. Public so tests can exercise the classification (including
/// "RPC method presence") without constructing a client or a live socket.
pub fn classify_rpc_proxy_error(err: &RpcProxyError) -> PythonAuthorityDenyReason {
    match err {
        RpcProxyError::Timeout { seconds, .. } => {
            PythonAuthorityDenyReason::Timeout { seconds: *seconds }
        }
        RpcProxyError::Rpc(inner) => {
            let message = inner.to_string();
            let lowered = message.to_ascii_lowercase();
            if message.contains("-32601") || lowered.contains("unknown command") {
                PythonAuthorityDenyReason::MethodNotFound
            } else {
                PythonAuthorityDenyReason::Transport(message)
            }
        }
    }
}

/// Validate one raw `revenue-fee-authority-status` response against exact
/// schema, `enabled=false`, nonnegative generation/timestamps, and bounded
/// observation age. `now` is the caller's own clock reading (never derived
/// from the response); `max_age_seconds` bounds `now - observed_at`
/// inclusively on both ends (a response from the future is exactly as
/// untrustworthy as a stale one).
///
/// Field checks run in a fixed order (object shape, then each field's
/// presence+type, then nonnegativity, then `enabled`, then staleness) so a
/// given bad response always reports the SAME reason.
pub fn validate_status(
    raw: &Value,
    now: i64,
    max_age_seconds: i64,
) -> Result<PythonAuthorityOff, PythonAuthorityDenyReason> {
    let obj = raw.as_object().ok_or_else(|| {
        PythonAuthorityDenyReason::MalformedResponse("response is not a JSON object".to_string())
    })?;

    let enabled = obj
        .get("enabled")
        .ok_or(PythonAuthorityDenyReason::MissingField("enabled"))?
        .as_bool()
        .ok_or(PythonAuthorityDenyReason::WrongFieldType("enabled"))?;

    let generation = obj
        .get("generation")
        .ok_or(PythonAuthorityDenyReason::MissingField("generation"))?
        .as_i64()
        .ok_or(PythonAuthorityDenyReason::WrongFieldType("generation"))?;

    let transitioned_at = obj
        .get("transitioned_at")
        .ok_or(PythonAuthorityDenyReason::MissingField("transitioned_at"))?
        .as_i64()
        .ok_or(PythonAuthorityDenyReason::WrongFieldType("transitioned_at"))?;

    let observed_at = obj
        .get("observed_at")
        .ok_or(PythonAuthorityDenyReason::MissingField("observed_at"))?
        .as_i64()
        .ok_or(PythonAuthorityDenyReason::WrongFieldType("observed_at"))?;

    if generation < 0 {
        return Err(PythonAuthorityDenyReason::NegativeField("generation"));
    }
    if transitioned_at < 0 {
        return Err(PythonAuthorityDenyReason::NegativeField("transitioned_at"));
    }
    if observed_at < 0 {
        return Err(PythonAuthorityDenyReason::NegativeField("observed_at"));
    }

    if enabled {
        return Err(PythonAuthorityDenyReason::StillEnabled);
    }

    let age_seconds = now - observed_at;
    if age_seconds < 0 || age_seconds > max_age_seconds {
        return Err(PythonAuthorityDenyReason::StaleObservation {
            age_seconds,
            max_age_seconds,
        });
    }

    Ok(PythonAuthorityOff {
        generation: generation as u64,
        transitioned_at,
        observed_at,
    })
}

/// Validate that two [`PythonAuthorityOff`] readings bracketing a batch
/// acquisition (a) report the SAME `generation` and `transitioned_at` --
/// see the module doc's "Two-read batch bracketing" -- and (b) that the
/// second reading is a genuinely later fetch, i.e. `second.observed_at`
/// strictly exceeds `first.observed_at`.
///
/// Fix-round I1: (b) is what makes "bracketing" real. Without it,
/// `validate_stable_epoch(&x, &x)` -- checking one reading against itself,
/// or against a clone/duplicate with an identical `observed_at` -- would
/// pass vacuously, since a value trivially agrees with itself on
/// `generation`/`transitioned_at`. A batch authorizer that (by bug or by
/// omission) never actually re-fetched status before dispatch would be
/// indistinguishable from one that correctly re-confirmed a stable epoch.
/// Requiring strict advancement means the second value MUST have come from
/// an actual later call to [`PythonAuthorityClient::fetch_validated_status`]
/// (or an equivalent independent [`validate_status`] call against a fresh
/// response) rather than a reused prior result.
///
/// Check order: epoch identity (`generation`/`transitioned_at`) first, then
/// the advancing-`observed_at` requirement -- so a reading that fails both
/// always reports [`PythonAuthorityDenyReason::UnstableEpoch`], the more
/// fundamental disagreement.
pub fn validate_stable_epoch(
    first: &PythonAuthorityOff,
    second: &PythonAuthorityOff,
) -> Result<(), PythonAuthorityDenyReason> {
    if first.generation != second.generation || first.transitioned_at != second.transitioned_at {
        return Err(PythonAuthorityDenyReason::UnstableEpoch {
            first: (first.generation, first.transitioned_at),
            second: (second.generation, second.transitioned_at),
        });
    }
    if second.observed_at <= first.observed_at {
        return Err(PythonAuthorityDenyReason::NonAdvancingObservation {
            first_observed_at: first.observed_at,
            second_observed_at: second.observed_at,
        });
    }
    Ok(())
}

/// A narrow, read-only client over the `lightning-rpc` socket: the only
/// RPC it can ever issue is [`AUTHORITY_STATUS_METHOD`], through the
/// shared timeout wrapper. See the module doc's "Structurally read-only".
#[derive(Debug, Clone)]
pub struct PythonAuthorityClient {
    socket_path: PathBuf,
    timeout_seconds: u64,
}

impl PythonAuthorityClient {
    /// Construction touches neither the filesystem nor a socket -- the
    /// path is only dialed inside [`Self::fetch_raw_status`].
    pub fn new(socket_path: PathBuf, timeout_seconds: u64) -> Self {
        Self {
            socket_path,
            timeout_seconds,
        }
    }

    /// The one RPC call this type can make: `revenue-fee-authority-status`
    /// with empty params, through `revops_rpc::call_with_timeout` -- the
    /// same wrapper `fee_evidence::prefetch_rpc` and
    /// `hydration::call_listforwards` use. Returns the raw JSON value
    /// un-validated; callers pass it to [`validate_status`].
    pub async fn fetch_raw_status(&self) -> Result<Value, PythonAuthorityDenyReason> {
        let socket_path = self.socket_path.clone();
        revops_rpc::call_with_timeout(
            AUTHORITY_STATUS_METHOD,
            self.timeout_seconds,
            call_status_rpc(socket_path),
        )
        .await
        .map_err(|e| classify_rpc_proxy_error(&e))
    }

    /// [`Self::fetch_raw_status`] followed by [`validate_status`] in one
    /// call -- the "request a fresh token" primitive the live batch
    /// authorizer uses both before assembling a batch and again
    /// immediately before dispatch (see the module doc's "Two-read batch
    /// bracketing"; pair the two results with [`validate_stable_epoch`]).
    pub async fn fetch_validated_status(
        &self,
        now: i64,
        max_age_seconds: i64,
    ) -> Result<PythonAuthorityOff, PythonAuthorityDenyReason> {
        let raw = self.fetch_raw_status().await?;
        validate_status(&raw, now, max_age_seconds)
    }

    /// Task 59 F5: perform fetch #1 and freeze it, the ORIGINATING
    /// client, and a monotonic open stamp into an [`OpenBracket`].
    /// Consumes the client: the bracket owns the only handle to the
    /// endpoint its close will re-read, so the second fetch structurally
    /// cannot be pointed anywhere else.
    pub async fn open_bracket(
        self,
        now: i64,
        max_age_seconds: i64,
    ) -> Result<OpenBracket, PythonAuthorityDenyReason> {
        let first = self.fetch_validated_status(now, max_age_seconds).await?;
        Ok(OpenBracket {
            client: self,
            first,
            opened_at: std::time::Instant::now(),
            max_age_seconds,
        })
    }
}

/// Task 59 §4.2: how long an OPEN bracket may live (monotonic) before
/// its close refuses without a second fetch.
pub const OPEN_BRACKET_MAX_LIFETIME: std::time::Duration = std::time::Duration::from_secs(30);

/// Fetch #1 proof (Task 59 F5). NOT `Clone`. Holds the ORIGINATING
/// client by value -- `close` re-reads exactly the endpoint that
/// produced the first reading; no client parameter exists to point it
/// anywhere else. Reuse after the consuming authorization is a compile
/// error:
///
/// ```compile_fail,E0382
/// async fn reuse(bracket: revops::python_authority::OpenBracket) {
///     let moved = bracket;
///     let _still_here = bracket; // use after move
///     drop(moved);
/// }
/// ```
#[derive(Debug)]
pub struct OpenBracket {
    client: PythonAuthorityClient,
    first: PythonAuthorityOff,
    opened_at: std::time::Instant,
    /// The exact `max_age_seconds` bound `validate_status` enforced at
    /// open -- stored so close revalidates the FIRST reading against the
    /// SAME bound, structurally.
    max_age_seconds: i64,
}

impl OpenBracket {
    /// Task 59 F5: fetch #2 happens HERE, against `self.client`, as the
    /// authorization path's LAST gate before minting. Consumes the
    /// bracket: one two-fetch proof closes at most once.
    ///
    /// Stale-open refusal comes FIRST (R2-F2), on both arms
    /// independently -- the monotonic open lifetime and the first
    /// reading's wall-clock age against authorization-time `now` -- and
    /// SKIPS fetch #2 entirely: a refused stale bracket has performed
    /// exactly one total fetch, a successful close exactly two.
    pub(crate) async fn close(
        self,
        now: i64,
    ) -> Result<BracketedAuthorityOff, PythonAuthorityDenyReason> {
        let open_age = self.opened_at.elapsed();
        if open_age > OPEN_BRACKET_MAX_LIFETIME {
            return Err(PythonAuthorityDenyReason::StaleOpenBracket {
                age_seconds: open_age.as_secs() as i64,
                max_seconds: OPEN_BRACKET_MAX_LIFETIME.as_secs() as i64,
            });
        }
        let first_age = now - self.first.observed_at;
        if first_age < 0 || first_age > self.max_age_seconds {
            return Err(PythonAuthorityDenyReason::StaleOpenBracket {
                age_seconds: first_age,
                max_seconds: self.max_age_seconds,
            });
        }
        let second = self
            .client
            .fetch_validated_status(now, self.max_age_seconds)
            .await?;
        validate_stable_epoch(&self.first, &second)?;
        Ok(BracketedAuthorityOff { second })
    }
}

/// Two-real-fetches proof (Task 59 F3): private fields, no public
/// constructor, NOT `Clone` -- it exists only as [`OpenBracket::close`]'s
/// success value, consumed by value exactly once inside the authorizer.
/// Forgery is a compile error:
///
/// ```compile_fail,E0063
/// let forged = revops::python_authority::BracketedAuthorityOff {};
/// ```
#[derive(Debug)]
pub struct BracketedAuthorityOff {
    second: PythonAuthorityOff,
}

impl BracketedAuthorityOff {
    /// The second (closing) reading's generation -- the value the minted
    /// authorization records as its Python-authority epoch.
    pub(crate) fn second_generation(&self) -> u64 {
        self.second.generation
    }
}

/// One RPC call over a fresh `cln_rpc::ClnRpc` connection (same
/// fresh-connection-per-call rationale as `hydration::call_listforwards`
/// and `fee_evidence::call_rpc`). Free function (not a method) so it can
/// be moved into the future `revops_rpc::call_with_timeout` awaits without
/// borrowing `self` across the `.await`.
async fn call_status_rpc(socket_path: PathBuf) -> anyhow::Result<Value> {
    use anyhow::Context;
    let mut rpc = cln_rpc::ClnRpc::new(&socket_path)
        .await
        .with_context(|| format!("connect lightning-rpc socket {}", socket_path.display()))?;
    rpc.call_raw::<Value, Value>(AUTHORITY_STATUS_METHOD, &serde_json::json!({}))
        .await
        .map_err(|e| anyhow::anyhow!("{AUTHORITY_STATUS_METHOD} RPC error: {e}"))
}
