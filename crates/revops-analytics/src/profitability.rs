//! Profitability P&L core: `ProfitabilityClass`, `ChannelRevenue`,
//! `ChannelProfitability`, `classify_channel` (port of
//! `modules/profitability_analyzer.py`).
//!
//! Task 4 (Wave 1) of
//! `docs/superpowers/plans/2026-07-17-phase3-analytics-budget.md`.
//!
//! ## Wave-1 cross-dependency note
//!
//! `channel_role()`/`role_30d()` conceptually delegate to the single
//! classification authority (`classification::ChannelRole` +
//! `classification::revenue_role_30d`, Task 2). Task 2 runs in a sibling
//! worktree in parallel and `crates/revops-analytics/src/classification.rs`
//! is still a doc-comment-only stub in THIS worktree, so importing from it
//! would not compile here. To keep this task's worktree independently
//! green (per the task contract) without touching a file Task 2 owns, this
//! module carries its own private `ChannelRole` + `revenue_role_30d`
//! mirroring the exact interface pinned in the plan's Task 2 section
//! (variant names, wire strings, thresholds). When Task 2 lands, whoever
//! integrates the two branches should delete this local copy and have
//! `channel_role`/`role_30d` return `classification::ChannelRole` /
//! call `classification::revenue_role_30d` instead — the shapes are
//! identical by construction, so it is a pure rename/re-export, not a
//! behavior change.

// =============================================================================
// ProfitabilityClass
// =============================================================================

/// Channel profitability classification (port of
/// `profitability_analyzer.ProfitabilityClass`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfitabilityClass {
    Profitable,
    BreakEven,
    Underwater,
    StagnantCandidate,
    Zombie,
}

impl ProfitabilityClass {
    /// Python `.value` (lowercase wire string; datastore telemetry).
    pub fn as_value(&self) -> &'static str {
        match self {
            ProfitabilityClass::Profitable => "profitable",
            ProfitabilityClass::BreakEven => "break_even",
            ProfitabilityClass::Underwater => "underwater",
            ProfitabilityClass::StagnantCandidate => "stagnant_candidate",
            ProfitabilityClass::Zombie => "zombie",
        }
    }

    /// Python `.name` (uppercase; conformance corpus wire shape).
    pub fn as_name(&self) -> &'static str {
        match self {
            ProfitabilityClass::Profitable => "PROFITABLE",
            ProfitabilityClass::BreakEven => "BREAK_EVEN",
            ProfitabilityClass::Underwater => "UNDERWATER",
            ProfitabilityClass::StagnantCandidate => "STAGNANT_CANDIDATE",
            ProfitabilityClass::Zombie => "ZOMBIE",
        }
    }
}

// =============================================================================
// Local ChannelRole mirror (see module doc comment above)
// =============================================================================

/// Mirrors `classification::ChannelRole` (Task 2) — see module doc comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelRole {
    InboundGateway,
    OutboundGateway,
    Balanced,
    Dormant,
}

impl ChannelRole {
    pub fn as_value(&self) -> &'static str {
        match self {
            ChannelRole::InboundGateway => "inbound_gateway",
            ChannelRole::OutboundGateway => "outbound_gateway",
            ChannelRole::Balanced => "balanced",
            ChannelRole::Dormant => "dormant",
        }
    }

    pub fn as_name(&self) -> &'static str {
        match self {
            ChannelRole::InboundGateway => "INBOUND_GATEWAY",
            ChannelRole::OutboundGateway => "OUTBOUND_GATEWAY",
            ChannelRole::Balanced => "BALANCED",
            ChannelRole::Dormant => "DORMANT",
        }
    }
}

const ROLE_MIN_FORWARDS_30D: i64 = 10;
const ROLE_DIRECTIONAL_RATIO: f64 = 0.70;

/// Windowed (30d) revenue-role classification (audit F2), mirroring
/// `classification.revenue_role_30d` 1:1. Falls back to `lifetime_role`
/// when no 30d window was fetched.
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

// =============================================================================
// msat -> sat rounding (frozen boundary semantics)
// =============================================================================

/// Ceiling msat -> sat: any non-zero base rounds UP to at least 1 sat.
/// Mirrors `utils.base_to_sats_ceil` (`-(-base // 1000)`). Caller must
/// guard `base_msat <= 0` before calling (ceiling is only meaningful for
/// strictly positive amounts here).
fn base_to_sats_ceil(base_msat: i64) -> i64 {
    -(-base_msat).div_euclid(1000)
}

/// Floor msat -> sat (truncate). Mirrors `utils.base_to_sats_floor`.
fn base_to_sats_floor(base_msat: i64) -> i64 {
    base_msat.div_euclid(1000)
}

// =============================================================================
// ChannelCosts
// =============================================================================

/// Cost tracking for a channel (port of `profitability_analyzer.ChannelCosts`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelCosts {
    pub channel_id: String,
    pub peer_id: String,
    pub open_cost_sats: i64,
    pub rebalance_cost_sats: i64,
    pub effective_rebalance_cost_sats: i64,
}

impl ChannelCosts {
    pub fn total_cost_sats(&self) -> i64 {
        self.open_cost_sats + self.rebalance_cost_sats
    }
}

// =============================================================================
// ChannelRevenue
// =============================================================================

/// Revenue tracking for a channel — msat-native (port of
/// `profitability_analyzer.ChannelRevenue`). Exact field set, lines 200-207.
///
/// bkpr attribution lesson: the EXIT channel earns the fee. `sourced_*`
/// fields are entry-side attribution for protection/valuation only — never
/// summed into fleet revenue (that would double-count).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelRevenue {
    pub channel_id: String,
    pub fees_earned_msat: i64,
    pub volume_routed_msat: i64,
    pub forward_count: i64,
    pub sourced_volume_msat: i64,
    pub sourced_fee_contribution_msat: i64,
    pub sourced_forward_count: i64,
}

impl ChannelRevenue {
    /// Fees earned in sats. Non-zero msat rounds up to at least 1 sat.
    pub fn fees_earned_sats(&self) -> i64 {
        if self.fees_earned_msat <= 0 {
            0
        } else {
            base_to_sats_ceil(self.fees_earned_msat)
        }
    }

    /// Volume routed in sats (floor — no ceiling needed for volume).
    pub fn volume_routed_sats(&self) -> i64 {
        base_to_sats_floor(self.volume_routed_msat)
    }

    /// Sourced fee contribution in sats. Non-zero msat rounds up to at
    /// least 1 sat.
    pub fn sourced_fee_contribution_sats(&self) -> i64 {
        if self.sourced_fee_contribution_msat <= 0 {
            0
        } else {
            base_to_sats_ceil(self.sourced_fee_contribution_msat)
        }
    }

    /// Sourced volume in sats (floor).
    pub fn sourced_volume_sats(&self) -> i64 {
        base_to_sats_floor(self.sourced_volume_msat)
    }

    /// Channel's valuation contribution in msat — max of earned vs sourced.
    ///
    /// VALUATION only, never fleet revenue: fleet revenue is the sum of
    /// `fees_earned` across EXIT channels (bkpr attributes the fee to the
    /// exit channel; summing `sourced_*` too would double-count).
    pub fn total_contribution_msat(&self) -> i64 {
        self.fees_earned_msat
            .max(self.sourced_fee_contribution_msat)
    }

    /// Channel's valuation contribution in sats (ceiling for non-zero).
    pub fn total_contribution_sats(&self) -> i64 {
        let msat = self.total_contribution_msat();
        if msat <= 0 {
            0
        } else {
            base_to_sats_ceil(msat)
        }
    }

    /// Total forwards: as exit + as entry.
    pub fn total_forward_count(&self) -> i64 {
        self.forward_count + self.sourced_forward_count
    }
}

// =============================================================================
// ChannelProfitability
// =============================================================================

/// Complete profitability analysis for a channel (port of
/// `profitability_analyzer.ChannelProfitability`). Exact field set,
/// lines 288-317.
#[derive(Debug, Clone, PartialEq)]
pub struct ChannelProfitability {
    pub channel_id: String,
    pub peer_id: String,
    pub capacity_sats: i64,
    pub costs: ChannelCosts,
    pub revenue: ChannelRevenue,
    pub net_profit_sats: i64,
    pub roi_percent: f64,
    pub classification: ProfitabilityClass,
    pub cost_per_sat_routed: f64,
    pub fee_per_sat_routed: f64,
    pub days_open: i64,
    pub last_routed: Option<i64>,
    pub marginal_profit_30d_sats: i64,
    pub rebalance_cost_30d_sats: i64,
    pub opener: String,
    pub contribution_30d_msat: i64,
    pub fees_earned_30d_msat: i64,
    pub sourced_fee_30d_msat: i64,
    pub forward_count_30d: i64,
    pub sourced_forward_count_30d: i64,
    pub window_30d_available: bool,
}

impl ChannelProfitability {
    /// Marginal ROI on a trailing 30-day window (operational view — only
    /// ongoing rebalance costs, no sunk open cost).
    ///
    /// `cost <= 0` -> 1.0 if profit > 0 else 0.0; else TRUE division
    /// `profit / max(1, cost)` as f64 (corpus s05 pins -300/600 = -0.5).
    pub fn marginal_roi(&self) -> f64 {
        if self.rebalance_cost_30d_sats <= 0 {
            if self.marginal_profit_30d_sats > 0 {
                1.0
            } else {
                0.0
            }
        } else {
            self.marginal_profit_30d_sats as f64 / self.rebalance_cost_30d_sats.max(1) as f64
        }
    }

    /// Marginal ROI as a percentage.
    pub fn marginal_roi_percent(&self) -> f64 {
        self.marginal_roi() * 100.0
    }

    /// Whether the channel is covering its ongoing (rebalance) costs.
    pub fn is_operationally_profitable(&self) -> bool {
        self.marginal_roi() >= 0.0
    }

    /// Whether `marginal_roi` rests on material evidence (audit F8): under
    /// 100 sats of 30d rebalance spend, the ratio swings wildly on a few
    /// sats of cost.
    pub fn marginal_roi_reliable(&self) -> bool {
        self.rebalance_cost_30d_sats >= 100
    }

    /// Lifetime flow-role classification based on directional forward
    /// counts (not volume): <10 total forwards -> Dormant; >70% in one
    /// direction -> the matching gateway role; else Balanced.
    pub fn channel_role(&self) -> ChannelRole {
        let total_forwards = self.revenue.total_forward_count();
        if total_forwards < 10 {
            return ChannelRole::Dormant;
        }
        let inbound_ratio = self.revenue.sourced_forward_count as f64 / total_forwards as f64;
        let outbound_ratio = self.revenue.forward_count as f64 / total_forwards as f64;
        if inbound_ratio > 0.70 {
            ChannelRole::InboundGateway
        } else if outbound_ratio > 0.70 {
            ChannelRole::OutboundGateway
        } else {
            ChannelRole::Balanced
        }
    }

    /// Total forwards in the trailing 30d window: exit + entry.
    pub fn total_forward_count_30d(&self) -> i64 {
        self.forward_count_30d + self.sourced_forward_count_30d
    }

    /// Windowed (30d) flow-role classification (audit F2); delegates to
    /// the classification authority's decision (see module doc comment for
    /// the Wave-1 local-mirror note).
    pub fn role_30d(&self) -> ChannelRole {
        revenue_role_30d(
            self.window_30d_available,
            self.forward_count_30d,
            self.sourced_forward_count_30d,
            self.channel_role(),
        )
    }
}

// =============================================================================
// _classify_channel, pure-ified
// =============================================================================

const PROFITABLE_ROI_THRESHOLD: f64 = 0.10;
const UNDERWATER_ROI_THRESHOLD: f64 = -0.10;

/// Diagnostic rebalance stats for the zombie-detection branch. Python's
/// `None` values (missing DB row / no successful diagnostic) map to `0`
/// for both fields, matching `diag_stats.get(...) or 0` in the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiagStats {
    pub attempt_count: i64,
    pub last_success_time: i64,
}

/// Evidence injected into `classify_channel` in place of the Python
/// function's inline wall-clock read and DB queries, so the decision stays
/// pure and replayable.
pub struct ClassifyEvidence<'a> {
    pub now: i64,
    /// Only consulted when `roi < UNDERWATER_ROI_THRESHOLD`. `None` behaves
    /// like `attempt_count: 0` (skips the zombie branches entirely, same
    /// as a channel with no diagnostic history).
    pub diag_stats: Option<&'a DiagStats>,
    /// From the fee strategy state's `v2_state_json` thompson_state.
    /// `None` is equivalent to Python's `variance` default of `10000`
    /// (i.e. no threshold widening).
    pub posterior_variance: Option<f64>,
    /// Trailing-30d total contribution (audit F3). `None` disables the
    /// "historically-profitable corpse" branch entirely.
    pub contribution_30d_msat: Option<i64>,
}

/// Classify a channel's profitability (port of
/// `ChannelProfitabilityAnalyzer._classify_channel`, pure-ified). Branch
/// order ported verbatim from lines 2659-2755 of
/// `modules/profitability_analyzer.py`.
///
/// `_net_profit` and `forward_count` are accepted (matching the Python
/// signature / golden fixture shape) but are not consulted by any branch —
/// same as the Python source.
pub fn classify_channel(
    roi: f64,
    _net_profit: i64,
    last_routed: Option<i64>,
    days_open: i64,
    _forward_count: i64,
    ev: &ClassifyEvidence,
) -> ProfitabilityClass {
    // Python truthiness trap: `if last_routed:` treats 0 as never-routed,
    // same as None.
    let truthy_last_routed = last_routed.filter(|&lr| lr != 0);

    let days_inactive = match truthy_last_routed {
        Some(lr) => (ev.now - lr).div_euclid(86_400),
        None => days_open,
    };

    // 1. ZOMBIE (Defibrillator refinement): underwater AND at least 2
    // diagnostic attempts in the trailing window.
    if roi < UNDERWATER_ROI_THRESHOLD {
        if let Some(diag) = ev.diag_stats {
            if diag.attempt_count >= 2 {
                if diag.last_success_time > 0 {
                    let hours_since_diag_success =
                        (ev.now - diag.last_success_time).div_euclid(3600);
                    if hours_since_diag_success > 48
                        && (truthy_last_routed.is_none()
                            || truthy_last_routed.unwrap() < diag.last_success_time)
                    {
                        return ProfitabilityClass::Zombie;
                    }
                } else if days_inactive >= 7 {
                    // No success in 2+ attempts, and truly inactive.
                    return ProfitabilityClass::Zombie;
                }
            }
        }
    }

    // 2. STAGNANT_CANDIDATE: 0 forwards in the last 7+ days AND unprofitable.
    if days_inactive >= 7 && roi < -0.10 {
        return ProfitabilityClass::StagnantCandidate;
    }

    // 3. DTS confidence widening: proven fee posteriors get wider bands so
    // temporary revenue dips don't trigger harsh reclassification.
    let mut profitable_thresh = PROFITABLE_ROI_THRESHOLD;
    let mut underwater_thresh = UNDERWATER_ROI_THRESHOLD;
    if let Some(variance) = ev.posterior_variance {
        if variance < 2500.0 {
            profitable_thresh *= 0.5;
            underwater_thresh *= 1.5;
        }
    }

    // 4. Audit F3: historically-profitable corpse. Inactivity judged
    // independently of lifetime ROI sign — a mature channel with zero 30d
    // contribution and 30+ inactive days is dead capital regardless of how
    // profitable it once was.
    if let Some(contribution) = ev.contribution_30d_msat {
        if contribution <= 0 && days_inactive >= 30 && days_open > 60 {
            return ProfitabilityClass::StagnantCandidate;
        }
    }

    if roi > profitable_thresh {
        ProfitabilityClass::Profitable
    } else if roi < underwater_thresh {
        ProfitabilityClass::Underwater
    } else {
        ProfitabilityClass::BreakEven
    }
}

/// Days since last routing activity (port of
/// `ChannelProfitabilityAnalyzer._days_since_routed`).
pub fn days_since_routed(now: i64, profitability: &ChannelProfitability) -> i64 {
    match profitability.last_routed.filter(|&lr| lr != 0) {
        Some(lr) => (now - lr).div_euclid(86_400),
        None => profitability.days_open,
    }
}
