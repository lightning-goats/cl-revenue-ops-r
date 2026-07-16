//! Frozen wire strings: `FeeReasonCode` values, damping `cap_reason`
//! strings, zero-flow guard tags, the governed-failure reason
//! `internal_error ({e})`, and the `FEE_LIMIT` clamp log format.
//!
//! Filled in by Phase 4 Task 3 (Wave 1).
//!
//! Only `FeeReasonCode` (port of `fee_controller.py:195-229`) is in scope
//! here — the other frozen strings named in the Phase 4 Global Constraints
//! (damping `cap_reason`, zero-flow guard tags, `internal_error`, the
//! `FEE_LIMIT` clamp log format) belong to the rail/orchestrator tasks that
//! own the code emitting them (Tasks 5/6/10), not this module.

/// Structured reason codes for fee adjustment decisions (verbatim port of
/// the Python `FeeReasonCode` enum, `fee_controller.py:195-229`). Wire
/// values are frozen strings — see Phase 4 Global Constraints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeeReasonCode {
    /// Peer has passive fee strategy.
    PolicyPassive,
    /// Peer has static fee target.
    PolicyStatic,
    /// Normal DTS posterior sample.
    DtsPidSample,
    /// Legacy 0-fee probe reason.
    ZeroFeeProbe,
    /// Legacy 0-fee probe success reason.
    ZeroFeeProbeSuccess,
    /// Bounded low-fee exploration.
    LowFeeExploration,
    /// Exploration saw traffic.
    LowFeeExplorationSuccess,
    /// Congestion-based fee surge.
    Congestion,
    /// Minimal nudge to refresh channel_update.
    GossipRefresh,
    /// Initial fee set on channel open.
    ChannelOpen,
    /// Hysteresis sleep mode active.
    SkipSleeping,
    /// Observation window too short.
    SkipWaitingTime,
    /// Not enough forwards for signal.
    SkipWaitingForwards,
    /// Calculated fee equals current fee.
    SkipFeeUnchanged,
}

impl FeeReasonCode {
    /// All 14 variants, in the same order as the Python enum body
    /// (`fee_controller.py:195-229`).
    pub const ALL: [FeeReasonCode; 14] = [
        FeeReasonCode::PolicyPassive,
        FeeReasonCode::PolicyStatic,
        FeeReasonCode::DtsPidSample,
        FeeReasonCode::ZeroFeeProbe,
        FeeReasonCode::ZeroFeeProbeSuccess,
        FeeReasonCode::LowFeeExploration,
        FeeReasonCode::LowFeeExplorationSuccess,
        FeeReasonCode::Congestion,
        FeeReasonCode::GossipRefresh,
        FeeReasonCode::ChannelOpen,
        FeeReasonCode::SkipSleeping,
        FeeReasonCode::SkipWaitingTime,
        FeeReasonCode::SkipWaitingForwards,
        FeeReasonCode::SkipFeeUnchanged,
    ];

    /// The frozen wire string (the Python enum's `.value`).
    pub const fn as_str(self) -> &'static str {
        match self {
            FeeReasonCode::PolicyPassive => "policy_passive",
            FeeReasonCode::PolicyStatic => "policy_static",
            FeeReasonCode::DtsPidSample => "dts_pid_sample",
            FeeReasonCode::ZeroFeeProbe => "zero_fee_probe",
            FeeReasonCode::ZeroFeeProbeSuccess => "zero_fee_probe_success",
            FeeReasonCode::LowFeeExploration => "low_fee_exploration",
            FeeReasonCode::LowFeeExplorationSuccess => "low_fee_exploration_success",
            FeeReasonCode::Congestion => "congestion",
            FeeReasonCode::GossipRefresh => "gossip_refresh",
            FeeReasonCode::ChannelOpen => "channel_open",
            FeeReasonCode::SkipSleeping => "skip_sleeping",
            FeeReasonCode::SkipWaitingTime => "skip_waiting_time",
            FeeReasonCode::SkipWaitingForwards => "skip_waiting_forwards",
            FeeReasonCode::SkipFeeUnchanged => "skip_fee_unchanged",
        }
    }

    /// Parse a wire string back to its variant (the from_dict/DB-decode
    /// direction). Returns `None` for an unrecognized string rather than
    /// panicking — callers own how to handle unknown legacy reason codes.
    pub fn from_str_wire(s: &str) -> Option<FeeReasonCode> {
        FeeReasonCode::ALL.into_iter().find(|v| v.as_str() == s)
    }
}

impl std::fmt::Display for FeeReasonCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn exactly_14_variants() {
        assert_eq!(FeeReasonCode::ALL.len(), 14);
    }

    #[test]
    fn wire_strings_are_frozen_and_unique() {
        let expected = [
            "policy_passive",
            "policy_static",
            "dts_pid_sample",
            "zero_fee_probe",
            "zero_fee_probe_success",
            "low_fee_exploration",
            "low_fee_exploration_success",
            "congestion",
            "gossip_refresh",
            "channel_open",
            "skip_sleeping",
            "skip_waiting_time",
            "skip_waiting_forwards",
            "skip_fee_unchanged",
        ];
        let actual: Vec<&str> = FeeReasonCode::ALL.iter().map(|v| v.as_str()).collect();
        assert_eq!(actual, expected);
        let unique: HashSet<&str> = actual.iter().copied().collect();
        assert_eq!(unique.len(), 14, "wire strings must be unique");
    }

    #[test]
    fn round_trips_through_wire_string() {
        for v in FeeReasonCode::ALL {
            assert_eq!(FeeReasonCode::from_str_wire(v.as_str()), Some(v));
        }
        assert_eq!(FeeReasonCode::from_str_wire("not_a_real_code"), None);
    }
}
