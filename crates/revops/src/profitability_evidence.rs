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
    LastRoutedUnavailable { scid: String, detail: String },
    DiagnosticsUnavailable { scid: String, detail: String },
    FeeStateUnavailable { scid: String, detail: String },
    OpenerUnavailable { scid: String, detail: String },
}

impl ProfitabilityEvidenceRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::LastRoutedUnavailable { .. } => "profitability_last_routed_unavailable",
            Self::DiagnosticsUnavailable { .. } => "profitability_diagnostics_unavailable",
            Self::FeeStateUnavailable { .. } => "profitability_fee_state_unavailable",
            Self::OpenerUnavailable { .. } => "profitability_opener_unavailable",
        }
    }

    /// Which channel this refusal is about. A fleet pass skips per
    /// channel, so a refusal that does not name one cannot be acted on.
    pub fn scid(&self) -> &str {
        match self {
            Self::LastRoutedUnavailable { scid, .. }
            | Self::DiagnosticsUnavailable { scid, .. }
            | Self::FeeStateUnavailable { scid, .. }
            | Self::OpenerUnavailable { scid, .. } => scid,
        }
    }

    pub fn detail(&self) -> &str {
        match self {
            Self::LastRoutedUnavailable { detail, .. }
            | Self::DiagnosticsUnavailable { detail, .. }
            | Self::FeeStateUnavailable { detail, .. }
            | Self::OpenerUnavailable { detail, .. } => detail,
        }
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
