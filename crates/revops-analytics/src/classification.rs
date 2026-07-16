//! Single classification authority (port of `modules/classification.py`,
//! refactor Phase 3B, Workstream A).
//!
//! Owns the plugin's channel-classification vocabulary and decisions:
//!
//! - [`ChannelState`] — liquidity/flow disposition (kalman + balance
//!   hysteresis). Consumed by fee flow_state, admission control, rebalance
//!   eligibility.
//! - [`ChannelRole`] — revenue role (directional forward activity).
//!   Consumed by close protection and the planner.
//!
//! These are DISTINCT concepts (spec Workstream A: "treat economic role and
//! lifecycle as distinct concepts"; likewise flow disposition vs revenue
//! role) — the unification is one AUTHORITY computing both from consistent
//! inputs, not one merged enum.
//!
//! Everything here is pure: no plugin, no rpc, no DB, no clock.

// =============================================================================
// Vocabulary
// =============================================================================

/// Classification of channel flow state.
///
/// - `Source`: Net outflow - channel is draining
/// - `Sink`: Net inflow - channel is filling
/// - `Balanced`: Roughly equal flow - ideal state
/// - `BalancedActive`: High-turnover two-way channel (balanced but busy)
/// - `Dormant`: Essentially no flow at all (turnover < 1%/day, no net trend)
/// - `Unknown`: Not enough data to classify
/// - `Congested`: HTLC slots near exhaustion (>80% used)
///
/// Note: the fee controller also recognizes 'router' as a flow_state, but
/// this classifier does not emit it yet — those branches are reserved.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ChannelState {
    Source,
    Sink,
    Balanced,
    BalancedActive,
    Dormant,
    Unknown,
    Congested,
}

impl ChannelState {
    /// Python `.value` — lowercase, the datastore telemetry wire shape.
    pub fn as_value(&self) -> &'static str {
        match self {
            ChannelState::Source => "source",
            ChannelState::Sink => "sink",
            ChannelState::Balanced => "balanced",
            ChannelState::BalancedActive => "balanced_active",
            ChannelState::Dormant => "dormant",
            ChannelState::Unknown => "unknown",
            ChannelState::Congested => "congested",
        }
    }

    /// Python `.name` — uppercase, the conformance corpus wire shape.
    pub fn as_name(&self) -> &'static str {
        match self {
            ChannelState::Source => "SOURCE",
            ChannelState::Sink => "SINK",
            ChannelState::Balanced => "BALANCED",
            ChannelState::BalancedActive => "BALANCED_ACTIVE",
            ChannelState::Dormant => "DORMANT",
            ChannelState::Unknown => "UNKNOWN",
            ChannelState::Congested => "CONGESTED",
        }
    }

    /// True for both `Balanced` and `BalancedActive`.
    pub fn is_balanced(&self) -> bool {
        matches!(self, ChannelState::Balanced | ChannelState::BalancedActive)
    }
}

/// Channel flow role classification based on directional activity.
///
/// Helps identify what purpose a channel serves in the routing topology:
/// - `InboundGateway`: Primarily sources volume from the network (>70% inbound)
/// - `OutboundGateway`: Primarily exits payments to the network (>70% outbound)
/// - `Balanced`: Roughly equal flow in both directions (within 70/30)
/// - `Dormant`: Little to no flow in either direction
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ChannelRole {
    InboundGateway,
    OutboundGateway,
    Balanced,
    Dormant,
}

impl ChannelRole {
    /// Python `.value` — lowercase, the datastore telemetry wire shape.
    pub fn as_value(&self) -> &'static str {
        match self {
            ChannelRole::InboundGateway => "inbound_gateway",
            ChannelRole::OutboundGateway => "outbound_gateway",
            ChannelRole::Balanced => "balanced",
            ChannelRole::Dormant => "dormant",
        }
    }

    /// Python `.name` — uppercase, the conformance corpus wire shape.
    pub fn as_name(&self) -> &'static str {
        match self {
            ChannelRole::InboundGateway => "INBOUND_GATEWAY",
            ChannelRole::OutboundGateway => "OUTBOUND_GATEWAY",
            ChannelRole::Balanced => "BALANCED",
            ChannelRole::Dormant => "DORMANT",
        }
    }
}

// =============================================================================
// Flow-state decision constants (moved verbatim from flow_analysis)
// =============================================================================

/// 1% of capacity per day.
pub const BALANCED_ACTIVE_TURNOVER_THRESHOLD: f64 = 0.01;
pub const DORMANT_KALMAN_RATIO_THRESHOLD: f64 = 0.01;
/// become SINK when outbound ratio rises above.
pub const SINK_ENTER_OUTBOUND_RATIO: f64 = 0.78;
/// stop being SINK when outbound ratio falls below.
pub const SINK_EXIT_OUTBOUND_RATIO: f64 = 0.72;
/// become SOURCE when outbound ratio falls below.
pub const SOURCE_ENTER_OUTBOUND_RATIO: f64 = 0.22;
/// stop being SOURCE when outbound ratio rises above.
pub const SOURCE_EXIT_OUTBOUND_RATIO: f64 = 0.28;
pub const KALMAN_BALANCE_VETO_RATIO: f64 = 0.05;

/// Revenue-role thresholds (from `ChannelProfitability.role_30d`).
pub const ROLE_MIN_FORWARDS_30D: i64 = 10;
pub const ROLE_DIRECTIONAL_RATIO: f64 = 0.70;

// =============================================================================
// Decisions
// =============================================================================

/// Classify via balance position when net flow is too weak to decide.
///
/// F1 (2026-06 audit): hysteresis bands keyed on the PREVIOUS class
/// prevent boundary channels from flapping each cycle, and a Kalman
/// direction veto stops a draining-but-currently-full channel from being
/// labelled SINK (or a filling-but-empty one SOURCE).
pub fn classify_balance_position(
    outbound_ratio: f64,
    previous_state: Option<&str>,
    kalman_ratio: f64,
    turnover: f64,
) -> ChannelState {
    let prev = previous_state.unwrap_or("").to_lowercase();

    let sink_band = if prev == ChannelState::Sink.as_value() {
        SINK_EXIT_OUTBOUND_RATIO
    } else {
        SINK_ENTER_OUTBOUND_RATIO
    };
    let source_band = if prev == ChannelState::Source.as_value() {
        SOURCE_EXIT_OUTBOUND_RATIO
    } else {
        SOURCE_ENTER_OUTBOUND_RATIO
    };

    // F1c: direction veto — never label against a measured flow trend.
    let sink_vetoed = kalman_ratio > KALMAN_BALANCE_VETO_RATIO;
    let source_vetoed = kalman_ratio < -KALMAN_BALANCE_VETO_RATIO;

    if outbound_ratio > sink_band && !sink_vetoed {
        return ChannelState::Sink;
    }
    if outbound_ratio < source_band && !source_vetoed {
        return ChannelState::Source;
    }

    if turnover > BALANCED_ACTIVE_TURNOVER_THRESHOLD {
        return ChannelState::BalancedActive;
    }
    // F6: turnover <= 1%/day here; with no measurable net trend either,
    // the channel is DORMANT (activates the fee controller's dormant
    // branches, e.g. the rebalance-cost floor exemption).
    if kalman_ratio.abs() < DORMANT_KALMAN_RATIO_THRESHOLD {
        return ChannelState::Dormant;
    }
    ChannelState::Balanced
}

/// The kalman-threshold flow decision (evidence supplied by the analyzer,
/// including any DTS-exploration threshold widening applied to
/// source/sink_threshold before this call).
pub fn flow_state(
    kalman_ratio: f64,
    source_threshold: f64,
    sink_threshold: f64,
    outbound_ratio: f64,
    previous_state: Option<&str>,
    turnover: f64,
) -> ChannelState {
    if kalman_ratio > source_threshold {
        return ChannelState::Source;
    }
    if kalman_ratio < sink_threshold {
        return ChannelState::Sink;
    }
    classify_balance_position(outbound_ratio, previous_state, kalman_ratio, turnover)
}

/// Windowed (30d) revenue-role classification (audit F2).
///
/// Same thresholds as the lifetime role (>=10 forwards, >70% directional)
/// computed from trailing-30d forward counts; falls back to the lifetime
/// role when no 30d window was fetched.
pub fn revenue_role_30d(
    window_30d_available: bool,
    forward_count_30d: i64,
    sourced_forward_count_30d: i64,
    lifetime_role: ChannelRole,
) -> ChannelRole {
    if !window_30d_available {
        return lifetime_role;
    }

    let total_forwards = forward_count_30d + sourced_forward_count_30d;
    if total_forwards < ROLE_MIN_FORWARDS_30D {
        return ChannelRole::Dormant;
    }

    let inbound_ratio = sourced_forward_count_30d as f64 / total_forwards as f64;
    let outbound_ratio = forward_count_30d as f64 / total_forwards as f64;
    if inbound_ratio > ROLE_DIRECTIONAL_RATIO {
        ChannelRole::InboundGateway
    } else if outbound_ratio > ROLE_DIRECTIONAL_RATIO {
        ChannelRole::OutboundGateway
    } else {
        ChannelRole::Balanced
    }
}
