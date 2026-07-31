//! F71-R23 slice 2: the per-channel evidence profitability needs before
//! the frozen classifier may be called.
//!
//! `profitability_assembler::assemble_fleet` hands `classify_channel` four
//! values it never looked up: `last_routed: None`, `diag_attempt_count: 0`,
//! `diag_last_success_time: 0`, `posterior_variance: None`. Three of those
//! coincide with Python's own defaults for "the query returned no row",
//! which is why they are invisible in every existing test. They are not
//! equivalent to Python, because Python ran the query first.
//!
//! `last_routed` is the one that does real damage. The classifier reads
//! `None` as never-routed and substitutes `days_open` for `days_inactive`
//! (py profitability_analyzer.py:2661-2663), so a mature channel that
//! routed yesterday is judged idle for its entire life and falls through
//! the STAGNANT_CANDIDATE branch. That is not underdetection; it is a
//! live, busy channel reported as dead capital, and downstream that is
//! close/redeploy evidence.
//!
//! The rule this module enforces is one level down from the analyze
//! slice's, and the same in shape:
//!
//! - **consulted, and the source genuinely has no row** -> Python's own
//!   default. A brand-new channel really has no routing history, no
//!   diagnostic attempts and no fee posterior; refusing there would make
//!   every new channel unevaluable.
//! - **could not consult the source at all** -> typed refusal. An I/O
//!   error is not evidence of absence.
//!
//! **Disclosed divergence.** Python's `_get_last_routing_time` wraps its
//! query in `except Exception: return None`
//! (profitability_analyzer.py:2585-2588), so a failed read silently
//! classifies every channel as never-routed -- the dead-capital verdict
//! above, produced by a database error. This module refuses instead, for
//! the same reason `econ_evidence` refuses on a failed `listfunds` rather
//! than reporting Python's zeros.

use revops_analytics::profitability::DiagStats;

/// Each source as the caller found it: `Err` = could not consult,
/// `Ok(None)` = consulted and there is no row.
pub struct ConsultedSources {
    /// py `get_last_forward_time_any_direction` (database.py:3453) --
    /// MAX over forwards in BOTH directions, unioned with the rolled-up
    /// daily stats so a pruned raw history does not read as inactivity.
    pub last_routed: Result<Option<i64>, String>,
    /// py `get_diagnostic_rebalance_stats(scid, days=14)`
    /// (database.py:2787).
    pub diag: Result<Option<DiagStats>, String>,
    /// py `get_fee_strategy_state(scid)` -> `v2_state_json` ->
    /// `thompson_state.posterior_variance` (profitability_analyzer.py:2723).
    pub posterior_variance: Result<Option<f64>, String>,
    /// Who funded the channel, from the live channel snapshot.
    pub opener: Result<Option<String>, String>,
}

/// The resolved evidence for one channel.
#[derive(Debug, Clone, PartialEq)]
pub struct ChannelEvidence {
    pub last_routed: Option<i64>,
    pub diag: DiagStats,
    pub posterior_variance: Option<f64>,
    pub opener: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfitabilityEvidenceRefusal {
    LastRoutedUnavailable {
        scid: String,
        detail: String,
    },
    DiagnosticsUnavailable {
        scid: String,
        detail: String,
    },
    FeeStateUnavailable {
        scid: String,
        detail: String,
    },
    OpenerUnavailable {
        scid: String,
        detail: String,
    },
    /// The production-database snapshot could not be read at all. Fleet
    /// level on purpose: no channel's revenue, costs or history is
    /// available, so there is no per-channel verdict to skip.
    SnapshotUnavailable {
        detail: String,
    },
    /// The observer store's fee state could not be read at all.
    FeeStoreUnavailable {
        detail: String,
    },
}

impl ProfitabilityEvidenceRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::LastRoutedUnavailable { .. } => "profitability_last_routed_unavailable",
            Self::DiagnosticsUnavailable { .. } => "profitability_diagnostics_unavailable",
            Self::FeeStateUnavailable { .. } => "profitability_fee_state_unavailable",
            Self::OpenerUnavailable { .. } => "profitability_opener_unavailable",
            Self::SnapshotUnavailable { .. } => "profitability_snapshot_unavailable",
            Self::FeeStoreUnavailable { .. } => "profitability_fee_store_unavailable",
        }
    }

    /// Which channel this refusal is about, when it is about one. A fleet
    /// pass skips per channel, so a per-channel refusal that does not name
    /// its channel cannot be acted on. The two store-level variants
    /// deliberately name none: attributing a whole-store failure to one
    /// channel would suggest the rest of the fleet was evaluated.
    pub fn scid(&self) -> Option<&str> {
        match self {
            Self::LastRoutedUnavailable { scid, .. }
            | Self::DiagnosticsUnavailable { scid, .. }
            | Self::FeeStateUnavailable { scid, .. }
            | Self::OpenerUnavailable { scid, .. } => Some(scid),
            Self::SnapshotUnavailable { .. } | Self::FeeStoreUnavailable { .. } => None,
        }
    }

    pub fn detail(&self) -> &str {
        match self {
            Self::LastRoutedUnavailable { detail, .. }
            | Self::DiagnosticsUnavailable { detail, .. }
            | Self::FeeStateUnavailable { detail, .. }
            | Self::OpenerUnavailable { detail, .. }
            | Self::SnapshotUnavailable { detail }
            | Self::FeeStoreUnavailable { detail } => detail,
        }
    }
}

/// Read one channel's fee posterior out of the observer store's
/// `v2_state_json` envelope, as py `_classify_channel` does
/// (profitability_analyzer.py:2715-2723).
///
/// `Ok(None)` is Python's `10000` default: at or above the `2500` widening
/// threshold, so it widens nothing. `Err` is the case Python cannot
/// express -- its whole posterior block sits under `except Exception:
/// pass`, so an unreadable envelope and a channel with no posterior both
/// arrive at the classifier as "no widening". Those are different facts:
/// one is a real answer about a new channel, the other is corrupt
/// controller state that the widening decision would otherwise rest on.
///
/// The `or` chain is Python's and is load-bearing in both directions: an
/// EMPTY nested `thompson_state` is falsy and falls through to the flat
/// pre-mirror row, while a NON-empty one wins outright and a missing
/// `posterior_variance` inside it takes the default rather than reaching
/// back to the flat row's (possibly stale) value.
/// Python's truthiness, which both `or`s in the chain above depend on.
/// `bool({})`, `bool([])`, `bool("")`, `bool(0)`, `bool(0.0)`,
/// `bool(False)` and `bool(None)` are all false, and each one makes the
/// chain fall through to the next term.
fn is_python_falsy(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => true,
        serde_json::Value::Bool(b) => !b,
        serde_json::Value::Number(n) => n.as_f64() == Some(0.0),
        serde_json::Value::String(s) => s.is_empty(),
        serde_json::Value::Array(a) => a.is_empty(),
        serde_json::Value::Object(o) => o.is_empty(),
    }
}

pub fn posterior_variance_from_v2_json(v2_state_json: &str) -> Result<Option<f64>, String> {
    // py: `fee_state.get('v2_state_json', '{}') or '{}'` -- an empty or
    // NULL column is an empty envelope, not a failure.
    let raw = if v2_state_json.is_empty() {
        "{}"
    } else {
        v2_state_json
    };
    let envelope: serde_json::Value = serde_json::from_str(raw)
        .map_err(|error| format!("v2_state_json is not valid JSON: {error}"))?;
    let envelope = envelope
        .as_object()
        .ok_or_else(|| "v2_state_json is not a JSON object".to_string())?;

    // py `(v2_data.get('fee_state') or {})`. A FALSY `fee_state` -- null,
    // false, 0, "", [], {} -- becomes an empty dict and the chain
    // continues to the flat row; that is not an error case in Python and
    // refusing there would deny a posterior Python honours. A TRUTHY
    // non-object raises AttributeError instead, which Python's blanket
    // handler turns into "no widening" -- and falling through to the flat
    // row there would be strictly worse than either behaviour, widening a
    // band from a stale pre-mirror posterior on the strength of corrupt
    // state.
    let fee_state = match envelope.get("fee_state") {
        Some(value) if !is_python_falsy(value) => Some(
            value
                .as_object()
                .ok_or_else(|| "fee_state is not a JSON object".to_string())?,
        ),
        _ => None,
    };

    // The second `or`, with the same falsy set.
    let nested = fee_state
        .and_then(|fee_state| fee_state.get("thompson_state"))
        .filter(|state| !is_python_falsy(state));

    let thompson = match nested.or_else(|| envelope.get("thompson_state")) {
        Some(state) => state,
        // py's `.get('thompson_state', {})` default: an ABSENT key yields
        // an empty dict, whose `posterior_variance` is the 10000 default.
        None => return Ok(None),
    };
    // Reached only when a value is present. `{}` is a dict and reads as
    // the default; null/0/""/[] are not, and `.get` on them raises.
    let thompson = thompson
        .as_object()
        .ok_or_else(|| "thompson_state is not a JSON object".to_string())?;

    match thompson.get("posterior_variance") {
        // Absent, or a JSON null: py's `isinstance` guard turns both into
        // "no widening", which is a real answer for a pre-posterior row.
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => value
            .as_f64()
            .map(Some)
            .ok_or_else(|| format!("posterior_variance is not a number: {value}")),
    }
}

/// Resolve one channel's classifier evidence, or say which source could
/// not be consulted.
pub fn channel_evidence(
    scid: &str,
    sources: ConsultedSources,
) -> Result<ChannelEvidence, ProfitabilityEvidenceRefusal> {
    let last_routed = sources.last_routed.map_err(|detail| {
        ProfitabilityEvidenceRefusal::LastRoutedUnavailable {
            scid: scid.to_string(),
            detail,
        }
    })?;

    // py `attempt_count` defaults to 0 (database.py:2811) and
    // `last_success_time` to 0 via `or 0`
    // (profitability_analyzer.py:2674). `attempt_count: 0` fails the
    // `>= 2` gate, so this is exactly Python's no-row behaviour -- but it
    // is reached only because the query RAN.
    let diag = sources
        .diag
        .map_err(
            |detail| ProfitabilityEvidenceRefusal::DiagnosticsUnavailable {
                scid: scid.to_string(),
                detail,
            },
        )?
        .unwrap_or(DiagStats {
            attempt_count: 0,
            last_success_time: 0,
        });

    // py defaults `variance` to 10000, which is >= the 2500 widening
    // threshold and so widens nothing. `None` is exactly that, and the
    // frozen classifier documents the equivalence.
    let posterior_variance = sources.posterior_variance.map_err(|detail| {
        ProfitabilityEvidenceRefusal::FeeStateUnavailable {
            scid: scid.to_string(),
            detail,
        }
    })?;

    // No Python default is honoured here. `opener` decides who paid the
    // opening fee, and defaulting it to "local" asserts this node funded a
    // channel it may not have funded -- a false statement about the
    // operator's costs, and one that is REPORTED, not merely fed to the
    // classifier.
    let opener = sources
        .opener
        .map_err(|detail| ProfitabilityEvidenceRefusal::OpenerUnavailable {
            scid: scid.to_string(),
            detail,
        })?
        .ok_or_else(|| ProfitabilityEvidenceRefusal::OpenerUnavailable {
            scid: scid.to_string(),
            detail: "the live channel snapshot carries no opener for this channel; \
                     defaulting it to \"local\" would claim this node paid the \
                     opening fee without evidence"
                .to_string(),
        })?;

    Ok(ChannelEvidence {
        last_routed,
        diag,
        posterior_variance,
        opener,
    })
}
