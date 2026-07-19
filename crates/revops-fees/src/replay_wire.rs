//! Strict parser for observational Python fee-cycle capture envelopes.
//!
//! The Python plugin remains the sole live authority. This module accepts
//! frozen, local replay inputs only: it has no RPC, database, scheduler, or
//! execution surface.

use std::collections::BTreeMap;
use std::fmt;

use revops_core::canonical::canonical_json;
use revops_econ::pyfloat::py_repr;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const REPLAY_SCHEMA_NAME: &str = "fee_cycle_replay";
pub const REPLAY_SCHEMA_VERSION: i64 = 0;
pub const MAX_REPLAY_ENVELOPE_BYTES: usize = 32 * 1024 * 1024;

const MANIFEST_SCHEMA_NAME: &str = "fee_cycle_capture_manifest";

pub type WireObject = BTreeMap<String, WireValue>;

/// JSON value accepted inside the flexible replay payload sections.
///
/// Bare JSON floats are deliberately absent. Python floats cross the wire as
/// one-key `{"__f__":"<CPython repr>"}` objects so canonical hashing remains
/// integer/string/object only on both sides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireValue {
    Null,
    Bool(bool),
    Integer(i64),
    String(String),
    Array(Vec<WireValue>),
    Object(WireObject),
    TaggedFloat(String),
}

impl Serialize for WireValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Null => serializer.serialize_none(),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Integer(value) => serializer.serialize_i64(*value),
            Self::String(value) => serializer.serialize_str(value),
            Self::Array(items) => items.serialize(serializer),
            Self::Object(entries) => entries.serialize(serializer),
            Self::TaggedFloat(rendered) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("__f__", rendered)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for WireValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(WireValueVisitor)
    }
}

struct WireValueVisitor;

impl<'de> Visitor<'de> for WireValueVisitor {
    type Value = WireValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .write_str("null, boolean, integer, string, array, object, or one-key tagged float")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(WireValue::Null)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(WireValue::Null)
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(WireValue::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(WireValue::Integer(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let value = i64::try_from(value)
            .map_err(|_| E::custom("replay wire integer exceeds signed 64-bit range"))?;
        Ok(WireValue::Integer(value))
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Err(E::custom(
            "untagged JSON float is forbidden in replay wire values",
        ))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(WireValue::String(value.to_string()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(WireValue::String(value))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut items = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
        while let Some(item) = sequence.next_element()? {
            items.push(item);
        }
        Ok(WireValue::Array(items))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut entries = BTreeMap::new();
        while let Some((key, value)) = map.next_entry::<String, WireValue>()? {
            if entries.insert(key.clone(), value).is_some() {
                return Err(de::Error::custom(format!(
                    "duplicate replay wire object key: {key}"
                )));
            }
        }

        if let Some(tagged) = entries.get("__f__") {
            if entries.len() != 1 {
                return Err(de::Error::custom(
                    "tagged float object must contain only __f__",
                ));
            }
            let WireValue::String(rendered) = tagged else {
                return Err(de::Error::custom("tagged float __f__ must be a string"));
            };
            if !is_cpython_float_repr(rendered) {
                return Err(de::Error::custom(format!(
                    "invalid finite CPython tagged float: {rendered}"
                )));
            }
            return Ok(WireValue::TaggedFloat(rendered.clone()));
        }

        Ok(WireValue::Object(entries))
    }
}

fn is_cpython_float_repr(rendered: &str) -> bool {
    rendered
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .is_some_and(|value| py_repr(value) == rendered)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeCycleReplayV0 {
    pub schema_name: String,
    pub schema_version: i64,
    pub capture_run_id: String,
    pub capture_seq: u64,
    pub cycle_id: String,
    pub producer: WireObject,
    pub started_at: String,
    pub configuration: WireObject,
    pub pre_state: WireObject,
    pub observations: WireObject,
    pub expected: WireObject,
    pub completeness: FeeCaptureCompletenessV0,
    pub payload_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeeCaptureCompletenessV0 {
    pub evaluated_channels: u64,
    pub terminal_outcomes: u64,
    pub evidence_entries: u64,
    pub clock_entries: u64,
    pub entropy_entries: u64,
    pub complete: bool,
    #[serde(flatten)]
    pub extra: WireObject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeCaptureManifestV0 {
    pub schema_name: String,
    pub schema_version: i64,
    pub capture_run_id: String,
    pub state: String,
    pub queue_drained: bool,
    pub started_at: String,
    pub updated_at: String,
    pub attempted: u64,
    pub completed: u64,
    pub failed: u64,
    pub dropped: u64,
    pub last_attempted_seq: Option<u64>,
    pub last_completed_seq: Option<u64>,
    pub retained_sequence_range: RetainedSequenceRangeV0,
    pub writer_health: String,
    pub last_error_category: Option<String>,
    pub attempts: Vec<FeeCaptureAttemptV0>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedSequenceRangeV0 {
    pub first: Option<u64>,
    pub last: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeCaptureAttemptV0 {
    pub capture_seq: u64,
    pub cycle_id: String,
    pub status: String,
    pub eligible: bool,
    pub filename: Option<String>,
    pub bytes: Option<u64>,
    pub error_category: Option<String>,
    pub error: Option<String>,
    pub rotation_error: Option<String>,
}

#[derive(Debug, Error)]
pub enum ReplayWireError {
    #[error("replay envelope is {actual} bytes; maximum is {maximum}")]
    EnvelopeTooLarge { actual: usize, maximum: usize },
    #[error("invalid replay JSON: {0}")]
    InvalidJson(String),
    #[error("schema_name must be {expected:?}, got {actual:?}")]
    WrongSchemaName {
        expected: &'static str,
        actual: String,
    },
    #[error("schema_version must be {expected}, got {actual}")]
    WrongSchemaVersion { expected: i64, actual: i64 },
    #[error("invalid payload digest: {0}")]
    InvalidDigest(String),
    #[error("payload digest mismatch")]
    DigestMismatch,
    #[error("cannot canonicalize replay body: {0}")]
    Canonical(String),
    #[error("capture completeness.complete must be true")]
    IncompleteCapture,
    #[error("completeness.{field} is {declared}, but the corresponding payload contains {actual}")]
    CountMismatch {
        field: &'static str,
        declared: u64,
        actual: usize,
    },
    #[error("capture manifest invariant failed: {0}")]
    ManifestInvariant(String),
}

/// Parse and fully validate one Python `fee_cycle_replay` v0 envelope.
pub fn parse_fee_capture(bytes: &[u8]) -> Result<FeeCycleReplayV0, ReplayWireError> {
    if bytes.len() > MAX_REPLAY_ENVELOPE_BYTES {
        return Err(ReplayWireError::EnvelopeTooLarge {
            actual: bytes.len(),
            maximum: MAX_REPLAY_ENVELOPE_BYTES,
        });
    }

    let capture: FeeCycleReplayV0 = serde_json::from_slice(bytes)
        .map_err(|error| ReplayWireError::InvalidJson(error.to_string()))?;
    validate_capture_identity(&capture)?;
    verify_payload_digest(&capture)?;
    validate_capture_completeness(&capture)?;
    Ok(capture)
}

fn validate_capture_identity(capture: &FeeCycleReplayV0) -> Result<(), ReplayWireError> {
    if capture.schema_name != REPLAY_SCHEMA_NAME {
        return Err(ReplayWireError::WrongSchemaName {
            expected: REPLAY_SCHEMA_NAME,
            actual: capture.schema_name.clone(),
        });
    }
    if capture.schema_version != REPLAY_SCHEMA_VERSION {
        return Err(ReplayWireError::WrongSchemaVersion {
            expected: REPLAY_SCHEMA_VERSION,
            actual: capture.schema_version,
        });
    }
    Ok(())
}

fn verify_payload_digest(capture: &FeeCycleReplayV0) -> Result<(), ReplayWireError> {
    if capture.payload_sha256.len() != 64
        || !capture
            .payload_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ReplayWireError::InvalidDigest(
            "payload_sha256 must be exactly 64 lowercase hexadecimal characters".to_string(),
        ));
    }

    let mut supplied = [0_u8; 32];
    hex::decode_to_slice(&capture.payload_sha256, &mut supplied)
        .map_err(|error| ReplayWireError::InvalidDigest(error.to_string()))?;

    let mut body = serde_json::to_value(capture)
        .map_err(|error| ReplayWireError::Canonical(error.to_string()))?;
    body.as_object_mut()
        .expect("FeeCycleReplayV0 serializes as an object")
        .remove("payload_sha256");
    let canonical =
        canonical_json(&body).map_err(|error| ReplayWireError::Canonical(error.to_string()))?;
    let expected = Sha256::digest(canonical.as_bytes());
    if !constant_time_digest_eq(expected.as_slice(), &supplied) {
        return Err(ReplayWireError::DigestMismatch);
    }
    Ok(())
}

fn constant_time_digest_eq(expected: &[u8], supplied: &[u8]) -> bool {
    if expected.len() != supplied.len() {
        return false;
    }
    expected
        .iter()
        .zip(supplied)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn validate_capture_completeness(capture: &FeeCycleReplayV0) -> Result<(), ReplayWireError> {
    if !capture.completeness.complete {
        return Err(ReplayWireError::IncompleteCapture);
    }

    validate_count(
        "evaluated_channels",
        capture.completeness.evaluated_channels,
        array_len(&capture.pre_state, "ordered_channels")?,
    )?;
    validate_count(
        "terminal_outcomes",
        capture.completeness.terminal_outcomes,
        array_len(&capture.expected, "ordered_outcomes")?,
    )?;
    validate_count(
        "evidence_entries",
        capture.completeness.evidence_entries,
        array_len(&capture.observations, "evidence")?,
    )?;
    validate_count(
        "clock_entries",
        capture.completeness.clock_entries,
        array_len(&capture.observations, "clock")?,
    )?;
    validate_count(
        "entropy_entries",
        capture.completeness.entropy_entries,
        array_len(&capture.observations, "entropy")?,
    )?;
    if capture.completeness.evaluated_channels != capture.completeness.terminal_outcomes {
        return Err(ReplayWireError::ManifestInvariant(
            "evaluated-channel and terminal-outcome counts differ".to_string(),
        ));
    }
    Ok(())
}

fn array_len(object: &WireObject, field: &'static str) -> Result<usize, ReplayWireError> {
    match object.get(field) {
        Some(WireValue::Array(items)) => Ok(items.len()),
        _ => Err(ReplayWireError::ManifestInvariant(format!(
            "required payload field {field} is not an array"
        ))),
    }
}

fn validate_count(
    field: &'static str,
    declared: u64,
    actual: usize,
) -> Result<(), ReplayWireError> {
    if usize::try_from(declared).ok() == Some(actual) {
        return Ok(());
    }
    Err(ReplayWireError::CountMismatch {
        field,
        declared,
        actual,
    })
}

/// Validate that a frozen manifest and its retained captures form one closed,
/// drained, lossless, strictly consecutive run.
pub fn validate_capture_manifest(
    manifest: &FeeCaptureManifestV0,
    captures: &[FeeCycleReplayV0],
) -> Result<(), ReplayWireError> {
    manifest_require(
        manifest.schema_name == MANIFEST_SCHEMA_NAME,
        format!(
            "schema_name must be {MANIFEST_SCHEMA_NAME:?}, got {:?}",
            manifest.schema_name
        ),
    )?;
    manifest_require(
        manifest.schema_version == REPLAY_SCHEMA_VERSION,
        format!(
            "schema_version must be {REPLAY_SCHEMA_VERSION}, got {}",
            manifest.schema_version
        ),
    )?;
    manifest_require(
        manifest.state == "closed",
        format!("state must be closed, got {:?}", manifest.state),
    )?;
    manifest_require(manifest.queue_drained, "queue_drained must be true")?;
    manifest_require(
        manifest.failed == 0,
        format!("failed must be zero, got {}", manifest.failed),
    )?;
    manifest_require(
        manifest.dropped == 0,
        format!("dropped must be zero, got {}", manifest.dropped),
    )?;
    manifest_require(
        manifest.attempted == manifest.completed,
        format!(
            "attempted {} does not equal completed {}",
            manifest.attempted, manifest.completed
        ),
    )?;
    manifest_require(
        manifest.writer_health == "healthy",
        format!(
            "writer_health must be healthy, got {:?}",
            manifest.writer_health
        ),
    )?;
    manifest_require(
        manifest.last_error_category.is_none(),
        "last_error_category must be null",
    )?;

    let expected_last = (manifest.completed > 0).then_some(manifest.completed);
    manifest_require(
        manifest.last_attempted_seq == expected_last,
        format!(
            "last_attempted_seq {:?} does not match completed sequence {:?}",
            manifest.last_attempted_seq, expected_last
        ),
    )?;
    manifest_require(
        manifest.last_completed_seq == expected_last,
        format!(
            "last_completed_seq {:?} does not match completed sequence {:?}",
            manifest.last_completed_seq, expected_last
        ),
    )?;

    if captures.is_empty() {
        manifest_require(
            manifest.completed == 0,
            "completed captures exist but no retained captures were supplied",
        )?;
        manifest_require(
            manifest.retained_sequence_range.first.is_none()
                && manifest.retained_sequence_range.last.is_none(),
            "empty captures require an empty retained_sequence_range",
        )?;
        manifest_require(
            manifest.attempts.is_empty(),
            "empty captures require no retained attempts",
        )?;
        return Ok(());
    }

    manifest_require(
        u64::try_from(captures.len())
            .ok()
            .is_some_and(|len| len <= manifest.completed),
        "retained capture count exceeds completed count",
    )?;

    let first_sequence = captures[0].capture_seq;
    let last_sequence = captures
        .last()
        .expect("nonempty captures have a last item")
        .capture_seq;
    manifest_require(
        manifest.retained_sequence_range.first == Some(first_sequence),
        format!(
            "retained_sequence_range.first {:?} does not match first capture {}",
            manifest.retained_sequence_range.first, first_sequence
        ),
    )?;
    manifest_require(
        manifest.retained_sequence_range.last == Some(last_sequence),
        format!(
            "retained_sequence_range.last {:?} does not match last capture {}",
            manifest.retained_sequence_range.last, last_sequence
        ),
    )?;

    for (index, capture) in captures.iter().enumerate() {
        let offset = u64::try_from(index).map_err(|_| {
            ReplayWireError::ManifestInvariant("capture index exceeds u64".to_string())
        })?;
        let expected_sequence = first_sequence.checked_add(offset).ok_or_else(|| {
            ReplayWireError::ManifestInvariant("capture sequence arithmetic overflow".to_string())
        })?;
        manifest_require(
            capture.capture_run_id == manifest.capture_run_id,
            format!(
                "capture_run_id {:?} does not match manifest {:?}",
                capture.capture_run_id, manifest.capture_run_id
            ),
        )?;
        manifest_require(
            capture.capture_seq == expected_sequence,
            format!(
                "capture sequences must be strictly consecutive; expected {expected_sequence}, got {}",
                capture.capture_seq
            ),
        )?;
        manifest_require(
            capture.cycle_id == format!("{}:{:08}", manifest.capture_run_id, capture.capture_seq),
            format!(
                "cycle_id {:?} does not match capture sequence {}",
                capture.cycle_id, capture.capture_seq
            ),
        )?;
    }

    let retained_count = u64::try_from(captures.len())
        .map_err(|_| ReplayWireError::ManifestInvariant("too many captures".to_string()))?;
    let expected_first = manifest
        .completed
        .checked_sub(retained_count)
        .and_then(|value| value.checked_add(1));
    manifest_require(
        expected_first == Some(first_sequence),
        format!(
            "retained captures do not end at completed sequence {}",
            manifest.completed
        ),
    )?;

    manifest_require(
        manifest.attempts.len() == captures.len(),
        format!(
            "retained attempt count {} does not match retained capture count {}",
            manifest.attempts.len(),
            captures.len()
        ),
    )?;
    for (attempt, capture) in manifest.attempts.iter().zip(captures) {
        manifest_require(
            attempt.capture_seq == capture.capture_seq,
            format!(
                "attempt sequence {} does not match capture sequence {}",
                attempt.capture_seq, capture.capture_seq
            ),
        )?;
        manifest_require(
            attempt.cycle_id == capture.cycle_id,
            format!(
                "attempt cycle_id {:?} does not match capture {:?}",
                attempt.cycle_id, capture.cycle_id
            ),
        )?;
        manifest_require(
            attempt.status == "completed",
            format!(
                "attempt {} status must be completed, got {:?}",
                attempt.capture_seq, attempt.status
            ),
        )?;
        manifest_require(
            attempt.eligible,
            format!("attempt {} must be eligible", attempt.capture_seq),
        )?;
    }

    Ok(())
}

fn manifest_require(condition: bool, message: impl Into<String>) -> Result<(), ReplayWireError> {
    if condition {
        Ok(())
    } else {
        Err(ReplayWireError::ManifestInvariant(message.into()))
    }
}
