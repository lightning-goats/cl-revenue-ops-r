//! Strict, offline replay of atomic Python fee-cycle captures.
//!
//! This module owns only in-memory adapters. It deliberately has no live
//! database, RPC, filesystem journal, ledger, socket, or process surface.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use revops_analytics::policy::{FeeStrategy, PeerPolicy, RebalanceMode};
use thiserror::Error;

use crate::cycle::{
    run_fee_cycle, ChannelInfo, ChannelStateRow, ControllerState, CycleDeps, DecisionClock,
    DecisionSummary, FeeCfgSnapshot, FeeEvidence, GossipRow, PeerFeeHistory, SkipGateEpoch,
    StateSink,
};
use crate::drain::NodeChannel;
use crate::execution::{
    decide_set_channel_fee, FeeAuthorizationRequest, FeeAuthorizationResult, FeeAuthorizer,
    FeeExecutionRequest, FeeExecutor, SetFeeDecision,
};
use crate::floors::{ChainCosts, FlowWindow, PeerLatency, RebalanceCostSample};
use crate::journal::FeeDecision;
use crate::pyjson::OValue;
use crate::pyrand::{DecisionEntropy, DecisionInputError};
use crate::replay_wire::{FeeCycleReplayV0, WireObject, WireValue};
use crate::state_store::{
    fee_state_to_capture_value, replay_import_channel_state, serialize_cycle_state_payload,
    ChannelCycleState, ChannelFeeState,
};
use crate::vegas::VegasReflexState;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ReplayError {
    #[error(
        "{family} mismatch: expected ordinal {expected_ordinal}, expected {expected}, \
         actual {actual}, path {path}"
    )]
    Transcript {
        family: &'static str,
        expected_ordinal: usize,
        expected: String,
        actual: String,
        path: String,
    },
    #[error("{path}: {message}")]
    Shape { path: String, message: String },
    #[error("{0}")]
    DecisionInput(String),
    #[error("value mismatch at {path}: expected {expected:?}, actual {actual:?}")]
    ValueMismatch {
        path: String,
        expected: WireValue,
        actual: WireValue,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumedTranscriptCounts {
    pub evidence: usize,
    pub clock: usize,
    pub entropy: usize,
    pub governor: usize,
    pub execution: usize,
    pub state_flush: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeeReplayResultV0 {
    pub ordered_outcomes: WireValue,
    pub decisions: Vec<WireValue>,
    pub decision_summary: WireValue,
    pub execution: WireValue,
    pub post_channel_state: WireValue,
    pub post_global: WireValue,
    pub consumed: ConsumedTranscriptCounts,
}

fn shape(path: impl Into<String>, message: impl Into<String>) -> ReplayError {
    ReplayError::Shape {
        path: path.into(),
        message: message.into(),
    }
}

fn transcript(
    family: &'static str,
    ordinal: usize,
    expected: impl Into<String>,
    actual: impl Into<String>,
    path: impl Into<String>,
) -> ReplayError {
    ReplayError::Transcript {
        family,
        expected_ordinal: ordinal,
        expected: expected.into(),
        actual: actual.into(),
        path: path.into(),
    }
}

fn object<'a>(value: &'a WireValue, path: &str) -> Result<&'a WireObject, ReplayError> {
    match value {
        WireValue::Object(value) => Ok(value),
        _ => Err(shape(path, "must be an object")),
    }
}

fn array<'a>(value: &'a WireValue, path: &str) -> Result<&'a [WireValue], ReplayError> {
    match value {
        WireValue::Array(value) => Ok(value),
        _ => Err(shape(path, "must be an array")),
    }
}

fn field<'a>(object: &'a WireObject, key: &str, path: &str) -> Result<&'a WireValue, ReplayError> {
    object
        .get(key)
        .ok_or_else(|| shape(format!("{path}.{key}"), "is required"))
}

fn string(value: &WireValue, path: &str) -> Result<String, ReplayError> {
    match value {
        WireValue::String(value) => Ok(value.clone()),
        _ => Err(shape(path, "must be a string")),
    }
}

fn integer(value: &WireValue, path: &str) -> Result<i64, ReplayError> {
    match value {
        WireValue::Integer(value) => Ok(*value),
        _ => Err(shape(path, "must be an integer")),
    }
}

fn boolean(value: &WireValue, path: &str) -> Result<bool, ReplayError> {
    match value {
        WireValue::Bool(value) => Ok(*value),
        _ => Err(shape(path, "must be a boolean")),
    }
}

fn optional_i64(value: &WireValue, path: &str) -> Result<Option<i64>, ReplayError> {
    match value {
        WireValue::Null => Ok(None),
        WireValue::Integer(value) => Ok(Some(*value)),
        _ => Err(shape(path, "must be an integer or null")),
    }
}

pub fn decode_tagged_float(value: &WireValue, path: &str) -> Result<f64, ReplayError> {
    let WireValue::TaggedFloat(rendered) = value else {
        return Err(shape(path, "must be a tagged float"));
    };
    rendered
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .ok_or_else(|| shape(path, "must contain a finite CPython float repr"))
}

fn number(value: &WireValue, path: &str) -> Result<f64, ReplayError> {
    match value {
        WireValue::Integer(value) => Ok(*value as f64),
        WireValue::TaggedFloat(_) => decode_tagged_float(value, path),
        _ => Err(shape(path, "must be an integer or tagged float")),
    }
}

fn wire_to_ovalue(value: &WireValue, path: &str) -> Result<OValue, ReplayError> {
    Ok(match value {
        WireValue::Null => OValue::Null,
        WireValue::Bool(value) => OValue::Bool(*value),
        WireValue::Integer(value) => OValue::Int(*value),
        WireValue::String(value) => OValue::Str(value.clone()),
        WireValue::TaggedFloat(_) => OValue::Float(decode_tagged_float(value, path)?),
        WireValue::Array(items) => OValue::Arr(
            items
                .iter()
                .enumerate()
                .map(|(index, item)| wire_to_ovalue(item, &format!("{path}[{index}]")))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        WireValue::Object(entries) => OValue::Obj(
            entries
                .iter()
                .map(|(key, item)| {
                    Ok((key.clone(), wire_to_ovalue(item, &format!("{path}.{key}"))?))
                })
                .collect::<Result<Vec<_>, ReplayError>>()?,
        ),
    })
}

fn ovalue_to_wire(value: &OValue) -> WireValue {
    match value {
        OValue::Null => WireValue::Null,
        OValue::Bool(value) => WireValue::Bool(*value),
        OValue::Int(value) => WireValue::Integer(*value),
        OValue::Float(value) => WireValue::TaggedFloat(revops_econ::pyfloat::py_repr(*value)),
        OValue::Str(value) => WireValue::String(value.clone()),
        OValue::Arr(items) => WireValue::Array(items.iter().map(ovalue_to_wire).collect()),
        OValue::Obj(entries) => WireValue::Object(
            entries
                .iter()
                .map(|(key, value)| (key.clone(), ovalue_to_wire(value)))
                .collect(),
        ),
    }
}

#[derive(Debug)]
struct Cursor {
    family: &'static str,
    entries: Vec<WireValue>,
    position: usize,
}

impl Cursor {
    fn new(observations: &WireObject, family: &'static str) -> Result<Self, ReplayError> {
        let path = format!("$.observations.{family}");
        let entries = array(field(observations, family, "$.observations")?, &path)?.to_vec();
        Ok(Self {
            family,
            entries,
            position: 0,
        })
    }

    fn take(&mut self, expected_name: &str, name_field: &str) -> Result<WireObject, ReplayError> {
        let ordinal = self.position;
        let base = format!("$.observations.{}[{ordinal}]", self.family);
        let Some(value) = self.entries.get(ordinal) else {
            return Err(transcript(
                self.family,
                ordinal,
                expected_name,
                "<missing>",
                base,
            ));
        };
        let entry = object(value, &base)?;
        let actual_ordinal = field(entry, "ordinal", &base)
            .and_then(|value| integer(value, &format!("{base}.ordinal")))?;
        if usize::try_from(actual_ordinal).ok() != Some(ordinal) {
            return Err(transcript(
                self.family,
                ordinal,
                expected_name,
                format!("ordinal {actual_ordinal}"),
                format!("{base}.ordinal"),
            ));
        }
        let actual_name = if name_field.is_empty() {
            expected_name.to_string()
        } else {
            field(entry, name_field, &base)
                .and_then(|value| string(value, &format!("{base}.{name_field}")))?
        };
        if actual_name != expected_name {
            return Err(transcript(
                self.family,
                ordinal,
                expected_name,
                actual_name,
                format!("{base}.{name_field}"),
            ));
        }
        self.position += 1;
        Ok(entry.clone())
    }

    fn finish(&self) -> Result<usize, ReplayError> {
        if self.position != self.entries.len() {
            let path = format!("$.observations.{}[{}]", self.family, self.position);
            let actual = self
                .entries
                .get(self.position)
                .and_then(|value| object(value, &path).ok())
                .and_then(|entry| entry.get("label").or_else(|| entry.get("op")))
                .and_then(|value| match value {
                    WireValue::String(value) => Some(value.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| "<entry>".to_string());
            return Err(transcript(
                self.family,
                self.position,
                "<end>",
                actual,
                path,
            ));
        }
        Ok(self.position)
    }
}

pub struct TranscriptClock {
    cursor: Cursor,
}

impl TranscriptClock {
    fn new(observations: &WireObject) -> Result<Self, ReplayError> {
        Ok(Self {
            cursor: Cursor::new(observations, "clock")?,
        })
    }
}

impl DecisionClock for TranscriptClock {
    fn now(&mut self, label: &str) -> Result<i64, DecisionInputError> {
        let ordinal = self.cursor.position;
        let entry = self
            .cursor
            .take(label, "label")
            .map_err(|error| DecisionInputError::new(error.to_string()))?;
        integer(
            entry
                .get("value")
                .ok_or_else(|| {
                    shape(
                        format!("$.observations.clock[{ordinal}].value"),
                        "is required",
                    )
                })
                .map_err(|error| DecisionInputError::new(error.to_string()))?,
            &format!("$.observations.clock[{ordinal}].value"),
        )
        .map_err(|error| DecisionInputError::new(error.to_string()))
    }
}

pub struct TranscriptEntropy {
    cursor: Cursor,
}

impl TranscriptEntropy {
    fn new(observations: &WireObject) -> Result<Self, ReplayError> {
        Ok(Self {
            cursor: Cursor::new(observations, "entropy")?,
        })
    }

    fn consume(&mut self, op: &str, label: &str, args: WireValue) -> Result<f64, ReplayError> {
        let ordinal = self.cursor.position;
        let base = format!("$.observations.entropy[{ordinal}]");
        let entry = self.cursor.take(op, "op")?;
        let actual_label = string(field(&entry, "label", &base)?, &format!("{base}.label"))?;
        if actual_label != label {
            return Err(transcript(
                "entropy",
                ordinal,
                label,
                actual_label,
                format!("{base}.label"),
            ));
        }
        let actual_args = field(&entry, "args", &base)?;
        if actual_args != &args {
            return Err(transcript(
                "entropy",
                ordinal,
                format!("{label} args {args:?}"),
                format!("{actual_label} args {actual_args:?}"),
                format!("{base}.args"),
            ));
        }
        decode_tagged_float(field(&entry, "result", &base)?, &format!("{base}.result"))
    }
}

impl DecisionEntropy for TranscriptEntropy {
    fn random(&mut self, label: &str) -> Result<f64, DecisionInputError> {
        self.consume("random", label, WireValue::Array(Vec::new()))
            .map_err(|error| DecisionInputError::new(error.to_string()))
    }

    fn gauss(&mut self, label: &str, mu: f64, sigma: f64) -> Result<f64, DecisionInputError> {
        self.consume(
            "gauss",
            label,
            WireValue::Array(vec![
                WireValue::TaggedFloat(revops_econ::pyfloat::py_repr(mu)),
                WireValue::TaggedFloat(revops_econ::pyfloat::py_repr(sigma)),
            ]),
        )
        .map_err(|error| DecisionInputError::new(error.to_string()))
    }
}

pub struct TranscriptEvidence {
    cursor: RefCell<Cursor>,
    channels: BTreeMap<String, ChannelInfo>,
    gossip: BTreeMap<String, Vec<GossipRow>>,
    our_id: String,
}

impl TranscriptEvidence {
    fn new(observations: &WireObject) -> Result<Self, ReplayError> {
        let mut cursor = Cursor::new(observations, "evidence")?;
        let channels_entry = cursor.take("channels_info", "op")?;
        require_args(&channels_entry, 0, "evidence", WireValue::Array(Vec::new()))?;
        reject_error_entry(&channels_entry, "evidence", 0)?;
        let raw_channels = field(&channels_entry, "result", "$.observations.evidence[0]")?.clone();
        let channels = decode_channels_info(&raw_channels, "$.observations.evidence[0].result")?;

        let mut gossip = BTreeMap::new();
        while let Some(WireValue::Object(entry)) = cursor.entries.get(cursor.position) {
            if entry.get("op") != Some(&WireValue::String("gossip_channels".to_string())) {
                break;
            }
            let ordinal = cursor.position;
            let entry = cursor.take("gossip_channels", "op")?;
            reject_error_entry(&entry, "evidence", ordinal)?;
            let args = array(
                field(
                    &entry,
                    "args",
                    &format!("$.observations.evidence[{ordinal}]"),
                )?,
                &format!("$.observations.evidence[{ordinal}].args"),
            )?;
            if args.len() != 1 {
                return Err(transcript(
                    "evidence",
                    ordinal,
                    "gossip_channels one peer argument",
                    format!("{args:?}"),
                    format!("$.observations.evidence[{ordinal}].args"),
                ));
            }
            let peer = string(
                &args[0],
                &format!("$.observations.evidence[{ordinal}].args[0]"),
            )?;
            let result = decode_gossip(
                field(
                    &entry,
                    "result",
                    &format!("$.observations.evidence[{ordinal}]"),
                )?,
                &format!("$.observations.evidence[{ordinal}].result"),
            )?;
            gossip.insert(peer, result);
        }
        let ordinal = cursor.position;
        let our = cursor.take("our_node_id", "op")?;
        require_args(&our, ordinal, "evidence", WireValue::Array(Vec::new()))?;
        reject_error_entry(&our, "evidence", ordinal)?;
        let our_id = string(
            field(
                &our,
                "result",
                &format!("$.observations.evidence[{ordinal}]"),
            )?,
            &format!("$.observations.evidence[{ordinal}].result"),
        )?;
        Ok(Self {
            cursor: RefCell::new(cursor),
            channels,
            gossip,
            our_id,
        })
    }

    fn consume(&self, op: &str, args: WireValue) -> Result<WireValue, DecisionInputError> {
        let mut cursor = self.cursor.borrow_mut();
        let ordinal = cursor.position;
        let entry = cursor
            .take(op, "op")
            .and_then(|entry| {
                require_args(&entry, ordinal, "evidence", args)?;
                reject_error_entry(&entry, "evidence", ordinal)?;
                field(
                    &entry,
                    "result",
                    &format!("$.observations.evidence[{ordinal}]"),
                )
                .cloned()
            })
            .map_err(|error| DecisionInputError::new(error.to_string()))?;
        Ok(entry)
    }
}

fn require_args(
    entry: &WireObject,
    ordinal: usize,
    family: &'static str,
    expected: WireValue,
) -> Result<(), ReplayError> {
    let base = format!("$.observations.{family}[{ordinal}]");
    let actual = field(entry, "args", &base)?;
    if actual != &expected {
        return Err(transcript(
            family,
            ordinal,
            format!("args {expected:?}"),
            format!("args {actual:?}"),
            format!("{base}.args"),
        ));
    }
    Ok(())
}

fn reject_error_entry(
    entry: &WireObject,
    family: &'static str,
    ordinal: usize,
) -> Result<(), ReplayError> {
    let base = format!("$.observations.{family}[{ordinal}]");
    for key in ["error", "fallback_error"] {
        if entry.contains_key(key) {
            return Err(transcript(
                family,
                ordinal,
                "result",
                key,
                format!("{base}.{key}"),
            ));
        }
    }
    Ok(())
}

impl FeeEvidence for TranscriptEvidence {
    fn our_node_id(&self) -> Result<String, DecisionInputError> {
        Ok(self.our_id.clone())
    }

    fn channel_states(&self) -> Result<Vec<ChannelStateRow>, DecisionInputError> {
        let value = self.consume("channel_states", WireValue::Array(Vec::new()))?;
        decode_channel_states(&value, "$.observations.evidence.result")
            .map_err(|error| DecisionInputError::new(error.to_string()))
    }

    fn channels_info(&self) -> Result<BTreeMap<String, ChannelInfo>, DecisionInputError> {
        Ok(self.channels.clone())
    }

    fn chain_costs(&self) -> Result<Option<ChainCosts>, DecisionInputError> {
        let value = self.consume("chain_costs", WireValue::Array(Vec::new()))?;
        if value == WireValue::Null {
            return Ok(None);
        }
        let object = object(&value, "$.observations.evidence.chain_costs")
            .map_err(|error| DecisionInputError::new(error.to_string()))?;
        Ok(Some(ChainCosts {
            open_cost_sats: object
                .get("open_cost_sats")
                .and_then(|value| match value {
                    WireValue::Integer(value) => Some(*value),
                    _ => None,
                })
                .unwrap_or(0),
            close_cost_sats: object
                .get("close_cost_sats")
                .and_then(|value| match value {
                    WireValue::Integer(value) => Some(*value),
                    _ => None,
                })
                .unwrap_or(0),
            sat_per_vbyte: number(
                field(
                    object,
                    "sat_per_vbyte",
                    "$.observations.evidence.chain_costs",
                )
                .map_err(|error| DecisionInputError::new(error.to_string()))?,
                "$.observations.evidence.chain_costs.sat_per_vbyte",
            )
            .map_err(|error| DecisionInputError::new(error.to_string()))?,
        }))
    }

    fn volume_since(&self, channel_id: &str, since: i64) -> Result<i64, DecisionInputError> {
        let value = self.consume(
            "volume_since",
            WireValue::Array(vec![
                WireValue::String(channel_id.to_string()),
                WireValue::Integer(since),
            ]),
        )?;
        integer(&value, "$.observations.evidence.volume_since.result")
            .map_err(|error| DecisionInputError::new(error.to_string()))
    }

    fn forward_count_since(&self, channel_id: &str, since: i64) -> Result<i64, DecisionInputError> {
        let value = self.consume(
            "forward_count_since",
            WireValue::Array(vec![
                WireValue::String(channel_id.to_string()),
                WireValue::Integer(since),
            ]),
        )?;
        integer(&value, "$.observations.evidence.forward_count_since.result")
            .map_err(|error| DecisionInputError::new(error.to_string()))
    }

    fn exploration_flag(&self, channel_id: &str) -> Result<bool, DecisionInputError> {
        let value = self.consume(
            "exploration_flag",
            WireValue::Array(vec![WireValue::String(channel_id.to_string())]),
        )?;
        boolean(&value, "$.observations.evidence.exploration_flag.result")
            .map_err(|error| DecisionInputError::new(error.to_string()))
    }

    fn clear_exploration_flag(&self, channel_id: &str) -> Result<(), DecisionInputError> {
        let _ = self.consume(
            "clear_exploration_flag",
            WireValue::Array(vec![WireValue::String(channel_id.to_string())]),
        )?;
        Ok(())
    }

    fn gossip_channels(&self, peer_id: &str) -> Result<Vec<GossipRow>, DecisionInputError> {
        self.gossip.get(peer_id).cloned().ok_or_else(|| {
            DecisionInputError::new(
                transcript(
                    "evidence",
                    self.cursor.borrow().position,
                    format!("prefetched gossip_channels {peer_id}"),
                    "<missing>",
                    "$.observations.evidence",
                )
                .to_string(),
            )
        })
    }

    fn peer_latency(&self, peer_id: &str) -> Result<Option<PeerLatency>, DecisionInputError> {
        let value = self.consume(
            "peer_latency",
            WireValue::Array(vec![WireValue::String(peer_id.to_string())]),
        )?;
        decode_peer_latency(&value).map_err(|error| DecisionInputError::new(error.to_string()))
    }

    fn channel_cost_history(
        &self,
        channel_id: &str,
        since: i64,
    ) -> Result<Vec<RebalanceCostSample>, DecisionInputError> {
        let value = self.consume(
            "channel_cost_history",
            WireValue::Array(vec![
                WireValue::String(channel_id.to_string()),
                WireValue::Integer(since),
            ]),
        )?;
        decode_cost_history(&value).map_err(|error| DecisionInputError::new(error.to_string()))
    }

    fn peer_fee_history(
        &self,
        peer_id: &str,
    ) -> Result<Option<PeerFeeHistory>, DecisionInputError> {
        let value = self.consume(
            "peer_fee_history",
            WireValue::Array(vec![WireValue::String(peer_id.to_string())]),
        )?;
        decode_peer_fee_history(&value).map_err(|error| DecisionInputError::new(error.to_string()))
    }

    fn last_forward_time(&self, channel_id: &str) -> Result<Option<i64>, DecisionInputError> {
        let value = self.consume(
            "last_forward_time",
            WireValue::Array(vec![WireValue::String(channel_id.to_string())]),
        )?;
        optional_i64(&value, "$.observations.evidence.last_forward_time.result")
            .map_err(|error| DecisionInputError::new(error.to_string()))
    }

    fn flow_window(&self, channel_id: &str) -> Result<Option<FlowWindow>, DecisionInputError> {
        let value = self.consume(
            "flow_window",
            WireValue::Array(vec![WireValue::String(channel_id.to_string())]),
        )?;
        decode_flow_window(&value).map_err(|error| DecisionInputError::new(error.to_string()))
    }

    fn policy(&self, peer_id: &str) -> Result<Option<PeerPolicy>, DecisionInputError> {
        let value = self.consume(
            "policy",
            WireValue::Array(vec![WireValue::String(peer_id.to_string())]),
        )?;
        decode_policy(&value, peer_id).map_err(|error| DecisionInputError::new(error.to_string()))
    }

    fn marginal_roi_percent(&self, channel_id: &str) -> Result<Option<f64>, DecisionInputError> {
        let value = self.consume(
            "marginal_roi_percent",
            WireValue::Array(vec![WireValue::String(channel_id.to_string())]),
        )?;
        match value {
            WireValue::Null => Ok(None),
            _ => number(
                &value,
                "$.observations.evidence.marginal_roi_percent.result",
            )
            .map(Some)
            .map_err(|error| DecisionInputError::new(error.to_string())),
        }
    }

    fn temporary_overlay_active(&self, channel_id: &str) -> Result<bool, DecisionInputError> {
        let value = self.consume(
            "temporary_overlay_active",
            WireValue::Array(vec![WireValue::String(channel_id.to_string())]),
        )?;
        boolean(
            &value,
            "$.observations.evidence.temporary_overlay_active.result",
        )
        .map_err(|error| DecisionInputError::new(error.to_string()))
    }

    fn mempool_ma_24h(&self) -> Result<f64, DecisionInputError> {
        let value = self.consume("mempool_ma_24h", WireValue::Array(Vec::new()))?;
        number(&value, "$.observations.evidence.mempool_ma_24h.result")
            .map_err(|error| DecisionInputError::new(error.to_string()))
    }

    fn node_channels(&self) -> Result<Vec<NodeChannel>, DecisionInputError> {
        let value = self.consume("node_channels", WireValue::Array(Vec::new()))?;
        decode_node_channels(&value).map_err(|error| DecisionInputError::new(error.to_string()))
    }
}

pub struct TranscriptAuthorizer {
    cursor: RefCell<Cursor>,
}

impl TranscriptAuthorizer {
    fn new(observations: &WireObject) -> Result<Self, ReplayError> {
        Ok(Self {
            cursor: RefCell::new(Cursor::new(observations, "governor")?),
        })
    }
}

impl FeeAuthorizer for TranscriptAuthorizer {
    fn authorize(
        &self,
        request: &FeeAuthorizationRequest,
    ) -> Result<FeeAuthorizationResult, DecisionInputError> {
        let mut cursor = self.cursor.borrow_mut();
        let ordinal = cursor.position;
        let base = format!("$.observations.governor[{ordinal}]");
        let entry = cursor
            .take("authorize", "")
            .map_err(|error| DecisionInputError::new(error.to_string()))?;
        let expected = WireValue::Object(
            [
                (
                    "channel_id".to_string(),
                    WireValue::String(request.channel_id.clone()),
                ),
                ("fee_ppm".to_string(), WireValue::Integer(request.fee_ppm)),
                (
                    "old_fee_ppm".to_string(),
                    request
                        .old_fee_ppm
                        .map(WireValue::Integer)
                        .unwrap_or(WireValue::Null),
                ),
                (
                    "reason".to_string(),
                    WireValue::String(request.reason.clone()),
                ),
                (
                    "reason_code".to_string(),
                    request
                        .reason_code
                        .clone()
                        .map(WireValue::String)
                        .unwrap_or(WireValue::Null),
                ),
            ]
            .into_iter()
            .collect(),
        );
        let actual = entry
            .get("request")
            .ok_or_else(|| shape(format!("{base}.request"), "is required"))
            .map_err(|error| DecisionInputError::new(error.to_string()))?;
        if actual != &expected {
            return Err(DecisionInputError::new(
                transcript(
                    "governor",
                    ordinal,
                    format!("authorize request {expected:?}"),
                    format!("authorize request {actual:?}"),
                    format!("{base}.request"),
                )
                .to_string(),
            ));
        }
        reject_error_entry(&entry, "governor", ordinal)
            .map_err(|error| DecisionInputError::new(error.to_string()))?;
        let result = object(
            entry
                .get("result")
                .ok_or_else(|| shape(format!("{base}.result"), "is required"))
                .map_err(|error| DecisionInputError::new(error.to_string()))?,
            &format!("{base}.result"),
        )
        .map_err(|error| DecisionInputError::new(error.to_string()))?;
        let authorized = boolean(
            result
                .get("authorized")
                .ok_or_else(|| shape(format!("{base}.result.authorized"), "is required"))
                .map_err(|error| DecisionInputError::new(error.to_string()))?,
            &format!("{base}.result.authorized"),
        )
        .map_err(|error| DecisionInputError::new(error.to_string()))?;
        let reason_code = string(
            result
                .get("reason")
                .ok_or_else(|| shape(format!("{base}.result.reason"), "is required"))
                .map_err(|error| DecisionInputError::new(error.to_string()))?,
            &format!("{base}.result.reason"),
        )
        .map_err(|error| DecisionInputError::new(error.to_string()))?;
        Ok(FeeAuthorizationResult {
            authorized,
            reason_code,
            trace: None,
        })
    }
}

pub struct TranscriptExecution {
    cursor: RefCell<Cursor>,
    consumed: RefCell<Vec<WireValue>>,
}

impl TranscriptExecution {
    fn new(observations: &WireObject) -> Result<Self, ReplayError> {
        Ok(Self {
            cursor: RefCell::new(Cursor::new(observations, "execution")?),
            consumed: RefCell::new(Vec::new()),
        })
    }
}

impl FeeExecutor for TranscriptExecution {
    fn execute(
        &self,
        request: &FeeExecutionRequest,
        cfg: &FeeCfgSnapshot,
        policy: Option<&PeerPolicy>,
    ) -> Result<SetFeeDecision, DecisionInputError> {
        let mut cursor = self.cursor.borrow_mut();
        let ordinal = cursor.position;
        let base = format!("$.observations.execution[{ordinal}]");
        let entry = cursor
            .take("execute", "")
            .map_err(|error| DecisionInputError::new(error.to_string()))?;
        let expected = ovalue_to_wire(&request.wire_request);
        let actual = entry
            .get("request")
            .ok_or_else(|| shape(format!("{base}.request"), "is required"))
            .map_err(|error| DecisionInputError::new(error.to_string()))?;
        if actual != &expected {
            return Err(DecisionInputError::new(
                transcript(
                    "execution",
                    ordinal,
                    format!("execute request {expected:?}"),
                    format!("execute request {actual:?}"),
                    format!("{base}.request"),
                )
                .to_string(),
            ));
        }
        reject_error_entry(&entry, "execution", ordinal)
            .map_err(|error| DecisionInputError::new(error.to_string()))?;
        let result_value = entry
            .get("result")
            .ok_or_else(|| shape(format!("{base}.result"), "is required"))
            .map_err(|error| DecisionInputError::new(error.to_string()))?;
        let result = object(result_value, &format!("{base}.result"))
            .map_err(|error| DecisionInputError::new(error.to_string()))?;
        let success = boolean(
            result
                .get("success")
                .ok_or_else(|| shape(format!("{base}.result.success"), "is required"))
                .map_err(|error| DecisionInputError::new(error.to_string()))?,
            &format!("{base}.result.success"),
        )
        .map_err(|error| DecisionInputError::new(error.to_string()))?;
        let fee_ppm = integer(
            result
                .get("fee_ppm")
                .ok_or_else(|| shape(format!("{base}.result.fee_ppm"), "is required"))
                .map_err(|error| DecisionInputError::new(error.to_string()))?,
            &format!("{base}.result.fee_ppm"),
        )
        .map_err(|error| DecisionInputError::new(error.to_string()))?;
        let message = string(
            result
                .get("message")
                .ok_or_else(|| shape(format!("{base}.result.message"), "is required"))
                .map_err(|error| DecisionInputError::new(error.to_string()))?,
            &format!("{base}.result.message"),
        )
        .map_err(|error| DecisionInputError::new(error.to_string()))?;
        let pure = decide_set_channel_fee(&request.decision, cfg, policy);
        self.consumed
            .borrow_mut()
            .push(WireValue::Object(entry.clone()));
        Ok(SetFeeDecision {
            success,
            clamped_fee_ppm: fee_ppm,
            message,
            clamp_log: pure.clamp_log,
        })
    }
}

#[derive(Default)]
pub struct MemoryStateSink {
    flushes: RefCell<Vec<Vec<(String, ChannelCycleState, ChannelFeeState)>>>,
}

impl StateSink for MemoryStateSink {
    fn flush_batch(&self, rows: &[(String, ChannelCycleState, ChannelFeeState)]) {
        self.flushes.borrow_mut().push(rows.to_vec());
    }
}

fn decode_cfg(configuration: &WireObject) -> Result<FeeCfgSnapshot, ReplayError> {
    let p = "$.configuration";
    Ok(FeeCfgSnapshot {
        min_fee_ppm: integer(
            field(configuration, "min_fee_ppm", p)?,
            &format!("{p}.min_fee_ppm"),
        )?,
        max_fee_ppm: integer(
            field(configuration, "max_fee_ppm", p)?,
            &format!("{p}.max_fee_ppm"),
        )?,
        min_fee_ppm_saturated: integer(
            field(configuration, "min_fee_ppm_saturated", p)?,
            &format!("{p}.min_fee_ppm_saturated"),
        )?,
        fee_interval: integer(
            field(configuration, "fee_interval", p)?,
            &format!("{p}.fee_interval"),
        )?,
        flow_interval: integer(
            field(configuration, "flow_interval", p)?,
            &format!("{p}.flow_interval"),
        )?,
        htlc_congestion_threshold: number(
            field(configuration, "htlc_congestion_threshold", p)?,
            &format!("{p}.htlc_congestion_threshold"),
        )?,
        market_fee_mode: string(
            field(configuration, "market_fee_mode", p)?,
            &format!("{p}.market_fee_mode"),
        )?,
        drain_fee_discount_max: number(
            field(configuration, "drain_fee_discount_max", p)?,
            &format!("{p}.drain_fee_discount_max"),
        )?,
        high_liquidity_threshold: number(
            field(configuration, "high_liquidity_threshold", p)?,
            &format!("{p}.high_liquidity_threshold"),
        )?,
        fee_profile: string(
            field(configuration, "fee_profile", p)?,
            &format!("{p}.fee_profile"),
        )?,
        base_fee_msat: integer(
            field(configuration, "base_fee_msat", p)?,
            &format!("{p}.base_fee_msat"),
        )?,
        enable_vegas_reflex: boolean(
            field(configuration, "enable_vegas_reflex", p)?,
            &format!("{p}.enable_vegas_reflex"),
        )?,
        enable_dynamic_htlcmax: wire_to_json(field(configuration, "enable_dynamic_htlcmax", p)?)?,
        htlcmax_source_pct: number(
            field(configuration, "htlcmax_source_pct", p)?,
            &format!("{p}.htlcmax_source_pct"),
        )?,
        htlcmax_sink_pct: number(
            field(configuration, "htlcmax_sink_pct", p)?,
            &format!("{p}.htlcmax_sink_pct"),
        )?,
        htlcmax_balanced_pct: number(
            field(configuration, "htlcmax_balanced_pct", p)?,
            &format!("{p}.htlcmax_balanced_pct"),
        )?,
        paused: boolean(field(configuration, "paused", p)?, &format!("{p}.paused"))?,
        node_drain_bias_enabled: boolean(
            field(configuration, "node_drain_bias_enabled", p)?,
            &format!("{p}.node_drain_bias_enabled"),
        )?,
        receivable_ratio_target: number(
            field(configuration, "receivable_ratio_target", p)?,
            &format!("{p}.receivable_ratio_target"),
        )?,
        receivable_ratio_floor: number(
            field(configuration, "receivable_ratio_floor", p)?,
            &format!("{p}.receivable_ratio_floor"),
        )?,
        econ_governor_fees_enabled: boolean(
            field(configuration, "econ_governor_fees_enabled", p)?,
            &format!("{p}.econ_governor_fees_enabled"),
        )?,
        authority_level: match field(configuration, "authority_level", p)? {
            WireValue::Null => None,
            value => Some(string(value, &format!("{p}.authority_level"))?),
        },
    })
}

fn wire_to_json(value: &WireValue) -> Result<serde_json::Value, ReplayError> {
    Ok(match value {
        WireValue::Null => serde_json::Value::Null,
        WireValue::Bool(value) => serde_json::Value::Bool(*value),
        WireValue::Integer(value) => serde_json::Value::Number((*value).into()),
        WireValue::String(value) => serde_json::Value::String(value.clone()),
        WireValue::TaggedFloat(_) => serde_json::Value::Number(
            serde_json::Number::from_f64(decode_tagged_float(value, "$.configuration")?)
                .ok_or_else(|| shape("$.configuration", "non-finite float"))?,
        ),
        WireValue::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(wire_to_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        WireValue::Object(entries) => serde_json::Value::Object(
            entries
                .iter()
                .map(|(key, value)| Ok((key.clone(), wire_to_json(value)?)))
                .collect::<Result<serde_json::Map<_, _>, ReplayError>>()?,
        ),
    })
}

fn import_state(capture: &FeeCycleReplayV0) -> Result<ControllerState, ReplayError> {
    let allowed_global = [
        "vegas_state",
        "vegas_wake_armed",
        "decision_summary",
        "random_state",
    ];
    for key in capture.pre_state.keys() {
        if !["global", "ordered_channels"].contains(&key.as_str()) {
            return Err(shape(
                format!("$.pre_state.{key}"),
                "is an unknown state field",
            ));
        }
    }
    let global = object(
        field(&capture.pre_state, "global", "$.pre_state")?,
        "$.pre_state.global",
    )?;
    for key in global.keys() {
        if !allowed_global.contains(&key.as_str()) {
            return Err(shape(
                format!("$.pre_state.global.{key}"),
                "is an unknown state field",
            ));
        }
    }
    for key in ["vegas_state", "vegas_wake_armed", "decision_summary"] {
        field(global, key, "$.pre_state.global")?;
    }
    let vegas_object = object(
        field(global, "vegas_state", "$.pre_state.global")?,
        "$.pre_state.global.vegas_state",
    )?;
    let vegas_keys = [
        "intensity",
        "last_sat_vb",
        "consecutive_spikes",
        "decay_rate",
        "last_update",
    ];
    validate_wire_fields(vegas_object, &vegas_keys, "$.pre_state.global.vegas_state")?;
    let summary_object = object(
        field(global, "decision_summary", "$.pre_state.global")?,
        "$.pre_state.global.decision_summary",
    )?;
    validate_wire_fields(
        summary_object,
        &["action", "reason", "dominant_input", "safety_block"],
        "$.pre_state.global.decision_summary",
    )?;
    let mut state = ControllerState::new();
    state.vegas = VegasReflexState {
        intensity: number(
            field(vegas_object, "intensity", "$.pre_state.global.vegas_state")?,
            "$.pre_state.global.vegas_state.intensity",
        )?,
        last_sat_vb: number(
            field(
                vegas_object,
                "last_sat_vb",
                "$.pre_state.global.vegas_state",
            )?,
            "$.pre_state.global.vegas_state.last_sat_vb",
        )?,
        consecutive_spikes: integer(
            field(
                vegas_object,
                "consecutive_spikes",
                "$.pre_state.global.vegas_state",
            )?,
            "$.pre_state.global.vegas_state.consecutive_spikes",
        )?,
        decay_rate: number(
            field(vegas_object, "decay_rate", "$.pre_state.global.vegas_state")?,
            "$.pre_state.global.vegas_state.decay_rate",
        )?,
        last_update: integer(
            field(
                vegas_object,
                "last_update",
                "$.pre_state.global.vegas_state",
            )?,
            "$.pre_state.global.vegas_state.last_update",
        )?,
    };
    state.vegas_wake_armed = boolean(
        field(global, "vegas_wake_armed", "$.pre_state.global")?,
        "$.pre_state.global.vegas_wake_armed",
    )?;
    state.last_decision_summary =
        decode_summary(summary_object, "$.pre_state.global.decision_summary")?;

    let channels = array(
        field(&capture.pre_state, "ordered_channels", "$.pre_state")?,
        "$.pre_state.ordered_channels",
    )?;
    let mut seen = BTreeSet::new();
    for (index, value) in channels.iter().enumerate() {
        let path = format!("$.pre_state.ordered_channels[{index}]");
        let entry = object(value, &path)?;
        validate_wire_fields(
            entry,
            &[
                "channel_id",
                "peer_id",
                "channel_state",
                "channel_info",
                "cycle_state",
                "fee_state",
            ],
            &path,
        )?;
        let channel_id = string(
            field(entry, "channel_id", &path)?,
            &format!("{path}.channel_id"),
        )?;
        if !seen.insert(channel_id.clone()) {
            return Err(shape(
                format!("{path}.channel_id"),
                format!("duplicate channel_id {channel_id}"),
            ));
        }
        let channel_state = object(
            field(entry, "channel_state", &path)?,
            &format!("{path}.channel_state"),
        )?;
        let state_channel_id = string(
            field(
                channel_state,
                "channel_id",
                &format!("{path}.channel_state"),
            )?,
            &format!("{path}.channel_state.channel_id"),
        )?;
        if state_channel_id != channel_id {
            return Err(shape(
                path,
                "state channel order differs from ordered_channels",
            ));
        }
        let cycle_value = wire_to_ovalue(
            field(entry, "cycle_state", &path)?,
            &format!("{path}.cycle_state"),
        )?;
        let fee_value = wire_to_ovalue(
            field(entry, "fee_state", &path)?,
            &format!("{path}.fee_state"),
        )?;
        let (cycle, fee) = replay_import_channel_state(&cycle_value, &fee_value, &path)
            .map_err(|message| shape(path.clone(), message))?;
        state.skip_gate_prev.insert(
            channel_id.clone(),
            SkipGateEpoch {
                last_update: cycle.last_update,
                is_sleeping: cycle.is_sleeping,
            },
        );
        state.cycle_states.insert(channel_id.clone(), cycle);
        state.fee_states.insert(channel_id, fee);
    }
    Ok(state)
}

fn validate_wire_fields(object: &WireObject, keys: &[&str], path: &str) -> Result<(), ReplayError> {
    for key in object.keys() {
        if !keys.contains(&key.as_str()) {
            return Err(shape(format!("{path}.{key}"), "is an unknown state field"));
        }
    }
    for key in keys {
        field(object, key, path)?;
    }
    Ok(())
}

fn decode_summary(object: &WireObject, path: &str) -> Result<DecisionSummary, ReplayError> {
    Ok(DecisionSummary {
        action: string(field(object, "action", path)?, &format!("{path}.action"))?,
        reason: string(field(object, "reason", path)?, &format!("{path}.reason"))?,
        dominant_input: match field(object, "dominant_input", path)? {
            WireValue::Null => None,
            value => Some(string(value, &format!("{path}.dominant_input"))?),
        },
        safety_block: boolean(
            field(object, "safety_block", path)?,
            &format!("{path}.safety_block"),
        )?,
    })
}

fn summary_wire(summary: &DecisionSummary) -> WireValue {
    WireValue::Object(
        [
            (
                "action".to_string(),
                WireValue::String(summary.action.clone()),
            ),
            (
                "reason".to_string(),
                WireValue::String(summary.reason.clone()),
            ),
            (
                "dominant_input".to_string(),
                summary
                    .dominant_input
                    .clone()
                    .map(WireValue::String)
                    .unwrap_or(WireValue::Null),
            ),
            (
                "safety_block".to_string(),
                WireValue::Bool(summary.safety_block),
            ),
        ]
        .into_iter()
        .collect(),
    )
}

fn post_channel_state(
    capture: &FeeCycleReplayV0,
    state: &ControllerState,
) -> Result<WireValue, ReplayError> {
    let channels = array(
        field(&capture.pre_state, "ordered_channels", "$.pre_state")?,
        "$.pre_state.ordered_channels",
    )?;
    let mut output = Vec::new();
    for (index, entry) in channels.iter().enumerate() {
        let path = format!("$.pre_state.ordered_channels[{index}]");
        let entry = object(entry, &path)?;
        let channel_id = string(
            field(entry, "channel_id", &path)?,
            &format!("{path}.channel_id"),
        )?;
        let peer_id = string(field(entry, "peer_id", &path)?, &format!("{path}.peer_id"))?;
        let cycle = state
            .cycle_states
            .get(&channel_id)
            .ok_or_else(|| shape(&path, "post cycle state missing"))?;
        let fee = state
            .fee_states
            .get(&channel_id)
            .ok_or_else(|| shape(&path, "post fee state missing"))?;
        output.push(WireValue::Object(
            [
                ("channel_id".to_string(), WireValue::String(channel_id)),
                ("peer_id".to_string(), WireValue::String(peer_id)),
                (
                    "cycle_state".to_string(),
                    ovalue_to_wire(&serialize_cycle_state_payload(cycle)),
                ),
                (
                    "fee_state".to_string(),
                    ovalue_to_wire(&fee_state_to_capture_value(fee)),
                ),
            ]
            .into_iter()
            .collect(),
        ));
    }
    Ok(WireValue::Array(output))
}

fn ordered_outcomes(decisions: &[FeeDecision]) -> WireValue {
    WireValue::Array(
        decisions
            .iter()
            .map(|decision| {
                if decision.would_broadcast {
                    WireValue::Object(
                        [(
                            "adjustment".to_string(),
                            WireValue::Object(
                                [
                                    (
                                        "channel_id".to_string(),
                                        WireValue::String(decision.channel_id.clone()),
                                    ),
                                    (
                                        "peer_id".to_string(),
                                        WireValue::String(decision.peer_id.clone()),
                                    ),
                                    (
                                        "old_fee_ppm".to_string(),
                                        WireValue::Integer(decision.old_fee_ppm),
                                    ),
                                    (
                                        "new_fee_ppm".to_string(),
                                        WireValue::Integer(decision.new_fee_ppm),
                                    ),
                                    (
                                        "reason".to_string(),
                                        WireValue::String(decision.reason.clone()),
                                    ),
                                    (
                                        "algorithm_values".to_string(),
                                        ovalue_to_wire(&decision.algorithm_values),
                                    ),
                                    (
                                        "reason_code".to_string(),
                                        WireValue::String(decision.reason_code.clone()),
                                    ),
                                ]
                                .into_iter()
                                .collect(),
                            ),
                        )]
                        .into_iter()
                        .collect(),
                    )
                } else {
                    let reason = decision
                        .reason
                        .strip_prefix("skip: ")
                        .unwrap_or(&decision.reason);
                    WireValue::Object(
                        [(
                            "skip".to_string(),
                            WireValue::Object(
                                [("reason".to_string(), WireValue::String(reason.to_string()))]
                                    .into_iter()
                                    .collect(),
                            ),
                        )]
                        .into_iter()
                        .collect(),
                    )
                }
            })
            .collect(),
    )
}

fn post_global_wire(state: &ControllerState, cfg: &FeeCfgSnapshot) -> WireValue {
    WireValue::Object(
        [
            (
                "vegas_state".to_string(),
                WireValue::Object(
                    [
                        (
                            "intensity".to_string(),
                            WireValue::TaggedFloat(revops_econ::pyfloat::py_repr(
                                state.vegas.intensity,
                            )),
                        ),
                        (
                            "last_sat_vb".to_string(),
                            WireValue::TaggedFloat(revops_econ::pyfloat::py_repr(
                                state.vegas.last_sat_vb,
                            )),
                        ),
                        (
                            "consecutive_spikes".to_string(),
                            WireValue::Integer(state.vegas.consecutive_spikes),
                        ),
                        (
                            "decay_rate".to_string(),
                            WireValue::TaggedFloat(revops_econ::pyfloat::py_repr(
                                state.vegas.decay_rate,
                            )),
                        ),
                        (
                            "last_update".to_string(),
                            WireValue::Integer(state.vegas.last_update),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                ),
            ),
            (
                "vegas_wake_armed".to_string(),
                WireValue::Bool(state.vegas_wake_armed),
            ),
            (
                "decision_summary".to_string(),
                summary_wire(&state.last_decision_summary),
            ),
            (
                "drain_values".to_string(),
                WireValue::Object(
                    [
                        ("node_receivable_ratio".to_string(), WireValue::Null),
                        ("node_drain_pressure".to_string(), WireValue::Null),
                        (
                            "effective_drain_discount_max".to_string(),
                            WireValue::TaggedFloat(revops_econ::pyfloat::py_repr(
                                cfg.drain_fee_discount_max,
                            )),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                ),
            ),
        ]
        .into_iter()
        .collect(),
    )
}

fn compare(path: &str, expected: &WireValue, actual: &WireValue) -> Result<(), ReplayError> {
    if expected == actual {
        Ok(())
    } else {
        Err(ReplayError::ValueMismatch {
            path: path.to_string(),
            expected: expected.clone(),
            actual: actual.clone(),
        })
    }
}

pub fn replay_fee_capture(capture: &FeeCycleReplayV0) -> Result<FeeReplayResultV0, ReplayError> {
    let cfg = decode_cfg(&capture.configuration)?;
    let mut state = import_state(capture)?;
    let observations = &capture.observations;
    let evidence = TranscriptEvidence::new(observations)?;
    let mut clock = TranscriptClock::new(observations)?;
    let mut entropy = TranscriptEntropy::new(observations)?;
    let authorizer = TranscriptAuthorizer::new(observations)?;
    let executor = TranscriptExecution::new(observations)?;
    let sink = MemoryStateSink::default();
    let mut deps = CycleDeps {
        evidence: &evidence,
        cfg: &cfg,
        rng: &mut entropy,
        clock: &mut clock,
        authorizer: Some(&authorizer),
        executor: &executor,
        journal: None,
        state_sink: Some(&sink),
        min_competitors: usize::try_from(integer(
            field(
                &capture.configuration,
                "neighbor_median_min_competitors",
                "$.configuration",
            )?,
            "$.configuration.neighbor_median_min_competitors",
        )?)
        .map_err(|_| {
            shape(
                "$.configuration.neighbor_median_min_competitors",
                "must be positive",
            )
        })?,
    };
    let decisions = run_fee_cycle(&mut state, &mut deps)
        .map_err(|error| ReplayError::DecisionInput(error.to_string()))?;

    // Compare the semantic outputs before checking transcript exhaustion. A
    // captured negative execution/governor result may intentionally prevent
    // later clock reads; the resulting terminal-outcome drift is the primary
    // mismatch, while the untouched entries remain a secondary consequence.
    let outcomes = ordered_outcomes(&decisions);
    let expected_outcomes = field(&capture.expected, "ordered_outcomes", "$.expected")?;
    compare("$.expected.ordered_outcomes", expected_outcomes, &outcomes)?;

    let counts = ConsumedTranscriptCounts {
        evidence: evidence.cursor.borrow().finish()?,
        clock: clock.cursor.finish()?,
        entropy: entropy.cursor.finish()?,
        governor: authorizer.cursor.borrow().finish()?,
        execution: executor.cursor.borrow().finish()?,
        state_flush: sink.flushes.borrow().len(),
    };
    if counts.state_flush != 1 {
        return Err(transcript(
            "state_flush",
            counts.state_flush,
            "exactly one flush",
            format!("{} flushes", counts.state_flush),
            "$.expected.post_channel_state",
        ));
    }

    let post_channels = post_channel_state(capture, &state)?;
    let expected_post_channels = field(&capture.expected, "post_channel_state", "$.expected")?;
    compare(
        "$.expected.post_channel_state",
        expected_post_channels,
        &post_channels,
    )?;
    let post_global = post_global_wire(&state, &cfg);
    let expected_post_global = field(&capture.expected, "post_global", "$.expected")?;
    let expected_post_global_object = object(expected_post_global, "$.expected.post_global")?;
    let mut expected_without_random = expected_post_global_object.clone();
    expected_without_random.remove("random_state");
    compare(
        "$.expected.post_global",
        &WireValue::Object(expected_without_random),
        &post_global,
    )?;

    Ok(FeeReplayResultV0 {
        ordered_outcomes: outcomes,
        decisions: decisions.iter().map(FeeDecision::to_replay_wire).collect(),
        decision_summary: summary_wire(&state.last_decision_summary),
        execution: WireValue::Array(executor.consumed.into_inner()),
        post_channel_state: post_channels,
        post_global,
        consumed: counts,
    })
}

fn decode_channels_info(
    value: &WireValue,
    path: &str,
) -> Result<BTreeMap<String, ChannelInfo>, ReplayError> {
    let entries = object(value, path)?;
    entries
        .iter()
        .map(|(channel_id, value)| {
            let p = format!("{path}.{channel_id}");
            let object = object(value, &p)?;
            Ok((
                channel_id.clone(),
                ChannelInfo {
                    channel_id: string(
                        field(object, "channel_id", &p)?,
                        &format!("{p}.channel_id"),
                    )?,
                    short_channel_id: string(
                        field(object, "short_channel_id", &p)?,
                        &format!("{p}.short_channel_id"),
                    )?,
                    peer_id: string(field(object, "peer_id", &p)?, &format!("{p}.peer_id"))?,
                    capacity_sats: integer(
                        field(object, "capacity", &p)?,
                        &format!("{p}.capacity"),
                    )?,
                    spendable_msat: integer(
                        field(object, "spendable_msat", &p)?,
                        &format!("{p}.spendable_msat"),
                    )?,
                    receivable_msat: integer(
                        field(object, "receivable_msat", &p)?,
                        &format!("{p}.receivable_msat"),
                    )?,
                    fee_base_msat: integer(
                        field(object, "fee_base_msat", &p)?,
                        &format!("{p}.fee_base_msat"),
                    )?,
                    fee_proportional_millionths: integer(
                        field(object, "fee_proportional_millionths", &p)?,
                        &format!("{p}.fee_proportional_millionths"),
                    )?,
                    htlc_minimum_msat: integer(
                        field(object, "htlc_minimum_msat", &p)?,
                        &format!("{p}.htlc_minimum_msat"),
                    )?,
                    htlc_maximum_msat: integer(
                        field(object, "htlc_maximum_msat", &p)?,
                        &format!("{p}.htlc_maximum_msat"),
                    )?,
                    opener: string(field(object, "opener", &p)?, &format!("{p}.opener"))?,
                    has_htlc_data: boolean(
                        field(object, "has_htlc_data", &p)?,
                        &format!("{p}.has_htlc_data"),
                    )?,
                    max_accepted_htlcs: integer(
                        field(object, "max_accepted_htlcs", &p)?,
                        &format!("{p}.max_accepted_htlcs"),
                    )?,
                    our_htlcs_in_flight: integer(
                        field(object, "our_htlcs_in_flight", &p)?,
                        &format!("{p}.our_htlcs_in_flight"),
                    )?,
                },
            ))
        })
        .collect()
}

fn decode_channel_states(
    value: &WireValue,
    path: &str,
) -> Result<Vec<ChannelStateRow>, ReplayError> {
    array(value, path)?
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let p = format!("{path}[{index}]");
            let object = object(value, &p)?;
            Ok(ChannelStateRow {
                channel_id: string(field(object, "channel_id", &p)?, &format!("{p}.channel_id"))?,
                peer_id: string(field(object, "peer_id", &p)?, &format!("{p}.peer_id"))?,
                state: string(field(object, "state", &p)?, &format!("{p}.state"))?,
                updated_at: object
                    .get("updated_at")
                    .map(|v| optional_i64(v, &format!("{p}.updated_at")))
                    .transpose()?
                    .flatten(),
                kalman_flow_ratio: object
                    .get("kalman_flow_ratio")
                    .map(|v| number(v, &format!("{p}.kalman_flow_ratio")))
                    .transpose()?,
                kalman_velocity: object
                    .get("kalman_velocity")
                    .map(|v| number(v, &format!("{p}.kalman_velocity")))
                    .transpose()?,
            })
        })
        .collect()
}

fn decode_gossip(value: &WireValue, path: &str) -> Result<Vec<GossipRow>, ReplayError> {
    array(value, path)?
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let p = format!("{path}[{index}]");
            let object = object(value, &p)?;
            Ok(GossipRow {
                source: string(field(object, "source", &p)?, &format!("{p}.source"))?,
                active: boolean(field(object, "active", &p)?, &format!("{p}.active"))?,
                fee_per_millionth: integer(
                    field(object, "fee_per_millionth", &p)?,
                    &format!("{p}.fee_per_millionth"),
                )?,
                satoshis: object
                    .get("satoshis")
                    .map(|v| optional_i64(v, &format!("{p}.satoshis")))
                    .transpose()?
                    .flatten(),
                amount_msat: object
                    .get("amount_msat")
                    .map(|v| optional_i64(v, &format!("{p}.amount_msat")))
                    .transpose()?
                    .flatten(),
                last_update: integer(
                    field(object, "last_update", &p)?,
                    &format!("{p}.last_update"),
                )?,
                base_fee_msat: object
                    .get("base_fee_msat")
                    .map(|v| optional_i64(v, &format!("{p}.base_fee_msat")))
                    .transpose()?
                    .flatten(),
            })
        })
        .collect()
}

fn decode_policy(value: &WireValue, peer_id: &str) -> Result<Option<PeerPolicy>, ReplayError> {
    if value == &WireValue::Null {
        return Ok(None);
    }
    let object = object(value, "$.observations.evidence.policy.result")?;
    let strategy = match string(
        field(object, "strategy", "$.observations.evidence.policy.result")?,
        "$.observations.evidence.policy.result.strategy",
    )?
    .as_str()
    {
        "passive" => FeeStrategy::Passive,
        "static" => FeeStrategy::Static,
        "dynamic" => FeeStrategy::Dynamic,
        other => {
            return Err(shape(
                "$.observations.evidence.policy.result.strategy",
                format!("unknown strategy {other}"),
            ))
        }
    };
    let rebalance_mode = match object.get("rebalance_mode") {
        Some(WireValue::String(value)) if value == "disabled" => RebalanceMode::Disabled,
        _ => RebalanceMode::Enabled,
    };
    Ok(Some(PeerPolicy {
        peer_id: peer_id.to_string(),
        strategy,
        fee_ppm_target: object
            .get("fee_ppm_target")
            .map(|v| optional_i64(v, "$.observations.evidence.policy.result.fee_ppm_target"))
            .transpose()?
            .flatten(),
        fee_multiplier_min: object
            .get("fee_multiplier_min")
            .map(|v| match v {
                WireValue::Null => Ok(None),
                _ => number(
                    v,
                    "$.observations.evidence.policy.result.fee_multiplier_min",
                )
                .map(Some),
            })
            .transpose()?
            .flatten(),
        fee_multiplier_max: object
            .get("fee_multiplier_max")
            .map(|v| match v {
                WireValue::Null => Ok(None),
                _ => number(
                    v,
                    "$.observations.evidence.policy.result.fee_multiplier_max",
                )
                .map(Some),
            })
            .transpose()?
            .flatten(),
        rebalance_mode,
        tags: Vec::new(),
        updated_at: object
            .get("updated_at")
            .map(|v| integer(v, "$.observations.evidence.policy.result.updated_at"))
            .transpose()?
            .unwrap_or(0),
        expires_at: object
            .get("expires_at")
            .map(|v| optional_i64(v, "$.observations.evidence.policy.result.expires_at"))
            .transpose()?
            .flatten(),
    }))
}

fn decode_peer_latency(value: &WireValue) -> Result<Option<PeerLatency>, ReplayError> {
    if value == &WireValue::Null {
        return Ok(None);
    }
    let o = object(value, "$.observations.evidence.peer_latency.result")?;
    Ok(Some(PeerLatency {
        avg: number(
            field(o, "avg", "$.observations.evidence.peer_latency.result")?,
            "$.observations.evidence.peer_latency.result.avg",
        )?,
        std: number(
            field(o, "std", "$.observations.evidence.peer_latency.result")?,
            "$.observations.evidence.peer_latency.result.std",
        )?,
    }))
}

fn decode_cost_history(value: &WireValue) -> Result<Vec<RebalanceCostSample>, ReplayError> {
    array(value, "$.observations.evidence.channel_cost_history.result")?
        .iter()
        .map(|value| {
            let o = object(
                value,
                "$.observations.evidence.channel_cost_history.result[]",
            )?;
            Ok(RebalanceCostSample {
                cost_sats: integer(
                    field(
                        o,
                        "cost_sats",
                        "$.observations.evidence.channel_cost_history.result[]",
                    )?,
                    "$.observations.evidence.channel_cost_history.result[].cost_sats",
                )?,
                amount_sats: integer(
                    field(
                        o,
                        "amount_sats",
                        "$.observations.evidence.channel_cost_history.result[]",
                    )?,
                    "$.observations.evidence.channel_cost_history.result[].amount_sats",
                )?,
                timestamp: integer(
                    field(
                        o,
                        "timestamp",
                        "$.observations.evidence.channel_cost_history.result[]",
                    )?,
                    "$.observations.evidence.channel_cost_history.result[].timestamp",
                )?,
            })
        })
        .collect()
}

fn decode_peer_fee_history(value: &WireValue) -> Result<Option<PeerFeeHistory>, ReplayError> {
    if value == &WireValue::Null {
        return Ok(None);
    }
    let o = object(value, "$.observations.evidence.peer_fee_history.result")?;
    Ok(Some(PeerFeeHistory {
        confidence: string(
            field(
                o,
                "confidence",
                "$.observations.evidence.peer_fee_history.result",
            )?,
            "$.observations.evidence.peer_fee_history.result.confidence",
        )?,
        avg_fee_ppm: integer(
            field(
                o,
                "avg_fee_ppm",
                "$.observations.evidence.peer_fee_history.result",
            )?,
            "$.observations.evidence.peer_fee_history.result.avg_fee_ppm",
        )?,
    }))
}

fn decode_flow_window(value: &WireValue) -> Result<Option<FlowWindow>, ReplayError> {
    if value == &WireValue::Null {
        return Ok(None);
    }
    let values = array(value, "$.observations.evidence.flow_window.result")?;
    if values.len() != 3 {
        return Err(shape(
            "$.observations.evidence.flow_window.result",
            "must be the Python [out_sats, in_sats, count] tuple",
        ));
    }
    Ok(Some(FlowWindow {
        out_sats: integer(&values[0], "$.observations.evidence.flow_window.result[0]")?,
        in_sats: integer(&values[1], "$.observations.evidence.flow_window.result[1]")?,
    }))
}

fn decode_node_channels(value: &WireValue) -> Result<Vec<NodeChannel>, ReplayError> {
    array(value, "$.observations.evidence.node_channels.result")?
        .iter()
        .map(|value| {
            let o = object(value, "$.observations.evidence.node_channels.result[]")?;
            Ok(NodeChannel {
                state: string(
                    field(o, "state", "$.observations.evidence.node_channels.result[]")?,
                    "$.observations.evidence.node_channels.result[].state",
                )?,
                to_us_msat: integer(
                    field(
                        o,
                        "to_us_msat",
                        "$.observations.evidence.node_channels.result[]",
                    )?,
                    "$.observations.evidence.node_channels.result[].to_us_msat",
                )?,
                total_msat: integer(
                    field(
                        o,
                        "total_msat",
                        "$.observations.evidence.node_channels.result[]",
                    )?,
                    "$.observations.evidence.node_channels.result[].total_msat",
                )?,
            })
        })
        .collect()
}
