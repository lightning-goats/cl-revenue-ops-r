//! Port of `CapexBudgetEngine` (py `modules/capex_budget.py`, 789 LOC):
//! the unified capital-expenditure budget engine. Computes per-channel 30d
//! budgets (tiers proven/active/bootstrap/blocked with tier PPM caps),
//! fleet exploration budget (channel opens), tactical budget (Boltz
//! treasury), and fleet priority class (defensive/preservation/operational/
//! growth).
//!
//! # Purity contract
//!
//! Python's `CapexBudgetEngine` is documented as "pure calculation — no CLN
//! RPC calls", but its `compute_allocations` (py 142-330) still reaches into
//! `self._profitability` / `self._database` / `self._capital_efficiency`
//! objects mid-computation. This port takes that contract literally: all
//! evidence those calls would have fetched is gathered into [`CapexEvidence`]
//! *before* calling [`compute_allocations`], which is then a pure function
//! of `(evidence, config) -> CapexAllocations`. `None` fields in
//! `CapexEvidence` encode exactly the failure modes Python's `try/except`
//! blocks degrade to (e.g. `capex_by_channel_sats: None` mirrors a `None`
//! return from `_get_total_capex_by_channel`, py 743-757 — CB-4 fail-closed).
//!
//! All internal arithmetic is msat (`i64`); sats only appear at output
//! boundaries via [`revops_core::msat::base_to_sats_ceil`] (CEILING
//! rounding — py `base_to_sats_ceil`, zero msat -> zero sats, no false
//! floor).

use revops_core::msat::base_to_sats_ceil;
use std::collections::BTreeMap;

/// Millisatoshis per satoshi (py `modules.utils.MSAT_PER_SAT`).
pub const MSAT_PER_SAT: i64 = 1000;

fn msat_to_sats_ceil(msat: i64) -> i64 {
    base_to_sats_ceil(msat.max(0) as u64) as i64
}

// ---------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------

/// The subset of `Config`/`ConfigSnapshot` fields `compute_allocations`
/// reads (py `config.py` defaults cited per field).
#[derive(Debug, Clone, PartialEq)]
pub struct CapexConfig {
    /// py `capex_reinvestment_rate: float = 0.50`.
    pub capex_reinvestment_rate: f64,
    /// py `capex_bootstrap_bps: int = 10`.
    pub capex_bootstrap_bps: i64,
    /// py `capex_bootstrap_max_sats: int = 200`.
    pub capex_bootstrap_max_sats: i64,
    /// py `capex_grace_days: int = 14`.
    pub capex_grace_days: i64,
    /// py `capex_exploration_rate: float = 0.10`.
    pub capex_exploration_rate: f64,
    /// py `capex_tactical_rate: float = 0.15`.
    pub capex_tactical_rate: f64,
    /// py `capex_global_envelope_sats: int = 0` (0 = auto-computed).
    pub capex_global_envelope_sats: i64,
    /// py `daily_budget_sats: int = 5000`.
    pub daily_budget_sats: i64,
    /// py `weekly_budget_sats: int = 35000`.
    pub weekly_budget_sats: i64,
    /// py `min_wallet_reserve: int = 1_000_000`.
    pub min_wallet_reserve: i64,
    /// py `estimated_open_cost_sats: int = 5000`.
    pub estimated_open_cost_sats: i64,
}

impl Default for CapexConfig {
    fn default() -> Self {
        CapexConfig {
            capex_reinvestment_rate: 0.50,
            capex_bootstrap_bps: 10,
            capex_bootstrap_max_sats: 200,
            capex_grace_days: 14,
            capex_exploration_rate: 0.10,
            capex_tactical_rate: 0.15,
            capex_global_envelope_sats: 0,
            daily_budget_sats: 5000,
            weekly_budget_sats: 35000,
            min_wallet_reserve: 1_000_000,
            estimated_open_cost_sats: 5000,
        }
    }
}

// ---------------------------------------------------------------------
// Evidence (pre-fetched inputs; nothing in this module performs I/O)
// ---------------------------------------------------------------------

/// Trailing-30d windowed fields (audit F1). `None` on [`ChannelProfile`]
/// mirrors `window_30d_available=False` (or a producer that never set it) —
/// the engine falls back to lifetime aggregates (py `_has_30d_window` /
/// `_windowed_msat`, capex_budget.py 37-59).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window30d {
    pub contribution_30d_msat: i64,
    pub fees_earned_30d_msat: i64,
}

/// Per-channel profitability evidence (the fields `compute_channel_budget`
/// and `compute_allocations` read off Python's `prof` object, py
/// 179-234, 514-634).
#[derive(Debug, Clone, PartialEq)]
pub struct ChannelProfile {
    /// Lowercased classification string (`prof.classification`, py 524, 207).
    /// Expected values include `"zombie"`, `"underwater"`,
    /// `"stagnant_candidate"`, `"profitable"`, `"break_even"`.
    pub classification: String,
    pub capacity_sats: i64,
    pub days_open: i64,
    /// `prof.marginal_roi` — a fraction (0.20 == 20%), not a percentage.
    pub marginal_roi: f64,
    /// `prof.marginal_roi_reliable` (py `getattr(prof, 'marginal_roi_reliable', True)`).
    pub marginal_roi_reliable: bool,
    /// `prof.channel_role` (py `getattr(prof, 'channel_role', None)`), already
    /// resolved to its string value (e.g. `Some("inbound_gateway")`).
    pub channel_role: Option<String>,
    /// `prof.revenue.total_contribution_msat`.
    pub total_contribution_msat: i64,
    /// `prof.revenue.total_forward_count`.
    pub total_forward_count: i64,
    /// `prof.revenue.fees_earned_msat` (lifetime fallback for fleet funding).
    pub fees_earned_msat: i64,
    /// `prof.revenue.sourced_fee_contribution_msat` (audit F5 gateway floor).
    pub sourced_fee_contribution_msat: i64,
    /// `Some` iff `prof.window_30d_available is True` (py 43).
    pub window_30d: Option<Window30d>,
}

/// `FleetCapitalEfficiency.channel_efficiencies[ch_id]` (py
/// `capital_efficiency.py`, read via `getattr`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChannelEfficiency {
    pub is_dead_capital: bool,
    pub rpsd: f64,
}

/// `FleetCapitalEfficiency` snapshot (py, read via `getattr` with `{}`/`0.0`
/// fallbacks at every field).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FleetEfficiency {
    pub channel_efficiencies: BTreeMap<String, ChannelEfficiency>,
    pub median_rpsd: f64,
}

/// `get_spend_ledger_summary` result shape (py `database.py`); `None` at
/// the [`CapexEvidence`] level mirrors a DB read failure (CB-4).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SpendSummary {
    pub spent_by_category: BTreeMap<String, i64>,
    pub reserved_by_category: BTreeMap<String, i64>,
}

/// Every input `compute_allocations` needs, gathered up front. `None`
/// variants encode the specific fail-closed conditions the Python
/// try/except blocks degrade to.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CapexEvidence {
    /// `all_prof` (py 151-155, `self._profitability.analyze_all_channels()`).
    pub channels: BTreeMap<String, ChannelProfile>,
    /// `bleeder_status` per channel, present only when
    /// `self._profitability.get_bleeder_status(ch_id)` returned a bleeder
    /// (py 195-203). Only `"hard"` changes behavior; other values are
    /// recorded for diagnostics only in Python and are not modeled here.
    pub bleeder_status: BTreeMap<String, String>,
    /// `_get_total_capex_by_channel(window_days=30)` (py 743-757). `None` =
    /// DB read failed -> CB-4 fail-closed (`db_degraded=True`, ALL budgets
    /// zeroed this cycle). Values are in **sats** (matching the Python
    /// return shape), converted to msat internally.
    pub capex_by_channel_sats: Option<BTreeMap<String, i64>>,
    /// `_get_spend_ledger_summary(window_days=30)` (py 759-773). `None` =
    /// DB read failed -> CB-4 fail-closed.
    pub spend_summary: Option<SpendSummary>,
    /// `_get_confirmed_onchain_sats()` (py 704-709; exceptions become 0
    /// there — callers should resolve that upstream and pass the resolved
    /// value here).
    pub onchain_sats: i64,
    /// Per-channel 30d rebalance success rate, present only when the
    /// Python evidence had `total >= 3` samples (py 570-575) — diagnostic
    /// only, stored verbatim on [`ChannelCapexBudget::success_rate_30d`].
    pub success_rates: BTreeMap<String, f64>,
    /// `self._capital_efficiency.analyze()` result, or `None` when no
    /// analyzer is wired / it raised (py 164-169).
    pub fleet_efficiency: Option<FleetEfficiency>,
}

// ---------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------

/// `ChannelCapexBudget.tier` (py capex_budget.py 67, values assigned at
/// 599/604/613/540/544/549/618).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Proven,
    Active,
    Bootstrap,
    Blocked,
}

impl Tier {
    pub fn as_str(&self) -> &'static str {
        match self {
            Tier::Proven => "proven",
            Tier::Active => "active",
            Tier::Bootstrap => "bootstrap",
            Tier::Blocked => "blocked",
        }
    }
}

/// `ChannelCapexBudget.priority_class` / fleet-level priority (py
/// capex_budget.py 69, 683-696).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PriorityClass {
    Defensive,
    Preservation,
    Operational,
    Growth,
}

impl PriorityClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            PriorityClass::Defensive => "defensive",
            PriorityClass::Preservation => "preservation",
            PriorityClass::Operational => "operational",
            PriorityClass::Growth => "growth",
        }
    }
}

/// Port of `ChannelCapexBudget` (py capex_budget.py 62-78).
#[derive(Debug, Clone, PartialEq)]
pub struct ChannelCapexBudget {
    pub channel_id: String,
    pub budget_msat: i64,
    pub tier: Tier,
    pub tier_ppm: i64,
    pub priority_class: PriorityClass,
    pub success_rate_30d: Option<f64>,
    pub roi_multiplier: f64,
}

impl ChannelCapexBudget {
    /// A blocked budget with the dataclass defaults Python uses at every
    /// `return ChannelCapexBudget(channel_id=ch_id, tier="blocked")` call
    /// site that does not also override `priority_class` (py 548-550,
    /// 618-620): `priority_class` defaults to `"growth"`.
    fn blocked(channel_id: &str) -> Self {
        ChannelCapexBudget {
            channel_id: channel_id.to_string(),
            budget_msat: 0,
            tier: Tier::Blocked,
            tier_ppm: 0,
            priority_class: PriorityClass::Growth,
            success_rate_30d: None,
            roi_multiplier: 1.0,
        }
    }

    /// A blocked budget with an explicit priority class override (py
    /// 540-542 zombie, 544-546 hard bleeder: both pass
    /// `priority_class="defensive"`).
    fn blocked_with_priority(channel_id: &str, priority_class: PriorityClass) -> Self {
        ChannelCapexBudget {
            priority_class,
            ..ChannelCapexBudget::blocked(channel_id)
        }
    }

    /// `ChannelCapexBudget.budget_sats` (py 75-78): ceiling-rounded, zero
    /// msat yields zero sats (no false floor).
    pub fn budget_sats(&self) -> i64 {
        msat_to_sats_ceil(self.budget_msat)
    }
}

/// Port of `CapexAllocations` (py capex_budget.py 82-119): the complete
/// budget-allocation snapshot for one cycle.
#[derive(Debug, Clone, PartialEq)]
pub struct CapexAllocations {
    pub priority_class: PriorityClass,
    pub global_envelope_msat: i64,
    pub channel_budgets: BTreeMap<String, ChannelCapexBudget>,
    pub fleet_exploration_budget_msat: i64,
    pub tactical_budget_msat: i64,
    pub allocated_by_priority_msat: BTreeMap<PriorityClass, i64>,
    pub total_fleet_contribution_msat: i64,
    /// CB-4 fail-closed: `True` when spend history could not be read this
    /// cycle, so all budgets were zeroed (spend denied) rather than
    /// re-granted as if nothing had been spent.
    pub db_degraded: bool,
}

impl CapexAllocations {
    pub fn global_envelope_sats(&self) -> i64 {
        msat_to_sats_ceil(self.global_envelope_msat)
    }
    pub fn fleet_exploration_budget_sats(&self) -> i64 {
        msat_to_sats_ceil(self.fleet_exploration_budget_msat)
    }
    pub fn tactical_budget_sats(&self) -> i64 {
        msat_to_sats_ceil(self.tactical_budget_msat)
    }
    pub fn total_fleet_contribution_sats(&self) -> i64 {
        msat_to_sats_ceil(self.total_fleet_contribution_msat)
    }
    /// `get_channel_budget` (py 332-336): a synthetic blocked budget for
    /// unknown channel ids, mirroring the Python fallback.
    pub fn channel_budget(&self, channel_id: &str) -> ChannelCapexBudget {
        self.channel_budgets
            .get(channel_id)
            .cloned()
            .unwrap_or_else(|| ChannelCapexBudget::blocked(channel_id))
    }
}

// ---------------------------------------------------------------------
// Pure helpers (py module-level / static functions)
// ---------------------------------------------------------------------

/// py `_has_gateway_value` (static, capex_budget.py 666-681): lifetime
/// role == inbound_gateway OR lifetime sourced fee contribution > 100_000
/// msat (100 sats).
fn has_gateway_value(prof: &ChannelProfile) -> bool {
    if prof.channel_role.as_deref() == Some("inbound_gateway") {
        return true;
    }
    prof.sourced_fee_contribution_msat > 100_000
}

/// py `_get_efficiency_multiplier` (capex_budget.py 636-664).
fn efficiency_multiplier(
    channel_id: &str,
    fleet_efficiency: Option<&FleetEfficiency>,
    prof: Option<&ChannelProfile>,
) -> f64 {
    let Some(fleet_efficiency) = fleet_efficiency else {
        return 1.0;
    };
    let Some(channel_eff) = fleet_efficiency.channel_efficiencies.get(channel_id) else {
        return 1.0;
    };
    if channel_eff.is_dead_capital {
        // Audit F5: dead-capital zeroing ignored gateway protections.
        if prof.map(has_gateway_value).unwrap_or(false) {
            return 0.25;
        }
        return 0.0;
    }
    let median_rpsd = fleet_efficiency.median_rpsd;
    let rpsd = channel_eff.rpsd;
    if median_rpsd <= 0.0 {
        return 1.0;
    }
    if rpsd >= median_rpsd {
        1.0 + (0.5f64).min((rpsd / median_rpsd - 1.0) * 0.25)
    } else {
        (0.5f64).max(rpsd / median_rpsd)
    }
}

/// py `_detect_priority_class` (capex_budget.py 683-696).
fn detect_priority_class(
    has_hard_bleeders: bool,
    has_depleted_earners: bool,
    reserve_deficit: i64,
) -> PriorityClass {
    if has_hard_bleeders {
        PriorityClass::Defensive
    } else if has_depleted_earners {
        PriorityClass::Preservation
    } else if reserve_deficit > 0 {
        PriorityClass::Operational
    } else {
        PriorityClass::Growth
    }
}

/// py `_get_reserve_deficit` (capex_budget.py 698-702).
fn reserve_deficit(cfg: &CapexConfig, onchain_sats: i64) -> i64 {
    (cfg.min_wallet_reserve - onchain_sats).max(0)
}

/// py `_get_bootstrap_wallet_exploration_budget_msat` (capex_budget.py 711-722).
fn bootstrap_wallet_exploration_budget_msat(
    cfg: &CapexConfig,
    onchain_sats: i64,
    summary: &SpendSummary,
) -> i64 {
    let wallet_excess_sats = (onchain_sats - cfg.min_wallet_reserve).max(0);
    let reserved_open_sats = *summary
        .reserved_by_category
        .get("channel_open")
        .unwrap_or(&0);
    (wallet_excess_sats - reserved_open_sats).max(0) * MSAT_PER_SAT
}

/// py `_get_wallet_exploration_floor_msat` (capex_budget.py 724-741).
fn wallet_exploration_floor_msat(cfg: &CapexConfig, onchain_sats: i64) -> i64 {
    let wallet_excess_sats = (onchain_sats - cfg.min_wallet_reserve).max(0);
    if wallet_excess_sats <= 0 {
        return 0;
    }
    let open_fee_sats = cfg.estimated_open_cost_sats.max(0);
    wallet_excess_sats.min(open_fee_sats) * MSAT_PER_SAT
}

/// py `_apply_category_spend_remaining` (capex_budget.py 775-789).
fn apply_category_spend_remaining(budget_msat: i64, summary: &SpendSummary, category: &str) -> i64 {
    let consumed_sats = *summary.spent_by_category.get(category).unwrap_or(&0)
        + *summary.reserved_by_category.get(category).unwrap_or(&0);
    (budget_msat - consumed_sats * MSAT_PER_SAT).max(0)
}

/// py `_windowed_msat` (capex_budget.py 46-59), specialized: the Rust
/// [`ChannelProfile::window_30d`] already encodes "has window and is
/// numeric" as `Some`, so this is a plain fallback.
fn windowed_msat(window_val: Option<i64>, fallback_msat: i64) -> i64 {
    window_val.unwrap_or(fallback_msat)
}

// ---------------------------------------------------------------------
// Per-channel budget (py `_compute_channel_budget`, capex_budget.py 514-634)
// ---------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn compute_channel_budget(
    ch_id: &str,
    prof: &ChannelProfile,
    total_capex_30d_msat: i64,
    bleeder_status: Option<&str>,
    cfg: &CapexConfig,
    fleet_efficiency: Option<&FleetEfficiency>,
    success_rate_30d: Option<f64>,
) -> ChannelCapexBudget {
    let contribution_msat = prof.total_contribution_msat;
    // Audit F1: budgets are debited by 30d spend, so the funding side must
    // also be the trailing-30d contribution (py 531-533).
    let contribution_30d_msat = windowed_msat(
        prof.window_30d.map(|w| w.contribution_30d_msat),
        contribution_msat,
    );
    let total_fwd = prof.total_forward_count;
    let days_open = prof.days_open;

    // Blocked channels (py 539-550).
    if prof.classification == "zombie" {
        return ChannelCapexBudget::blocked_with_priority(ch_id, PriorityClass::Defensive);
    }
    if bleeder_status == Some("hard") {
        return ChannelCapexBudget::blocked_with_priority(ch_id, PriorityClass::Defensive);
    }
    if days_open < cfg.capex_grace_days && contribution_msat == 0 {
        return ChannelCapexBudget::blocked(ch_id);
    }

    // Audit F6: marginal-ROI multiplier (py 577-581). Python: `min(1.5,
    // max(0.25, 1.0 + float(marginal_roi)))` when reliable, else 1.0.
    let roi_mult = if prof.marginal_roi_reliable {
        (1.0 + prof.marginal_roi).clamp(0.25, 1.5)
    } else {
        1.0
    };

    // Proven budget (msat): 30d contribution x rate - 30d capex spent (py
    // 583-586). `int(contribution_30d_msat * reinvestment)` is Python float
    // division/multiplication truncated toward zero.
    let proven_budget_msat = ((contribution_30d_msat as f64 * cfg.capex_reinvestment_rate) as i64
        - total_capex_30d_msat)
        .max(0);

    // Bootstrap budget (msat): basis points of capacity (py 588-593).
    // `capacity_msat * cfg.capex_bootstrap_bps / 10000` is Python true
    // (float) division.
    let capacity_msat = prof.capacity_sats * MSAT_PER_SAT;
    let bootstrap_budget_msat = ((capacity_msat as f64 * cfg.capex_bootstrap_bps as f64 / 10000.0)
        as i64)
        .min(cfg.capex_bootstrap_max_sats * MSAT_PER_SAT);

    // Tier classification (py 595-620).
    let (tier, tier_ppm, priority, raw_budget_msat) = if contribution_30d_msat > 100 * MSAT_PER_SAT
    {
        (
            Tier::Proven,
            2000,
            PriorityClass::Preservation,
            proven_budget_msat,
        )
    } else if total_fwd > 5 {
        let raw = if proven_budget_msat > 0 {
            proven_budget_msat.max((bootstrap_budget_msat - total_capex_30d_msat).max(0))
        } else {
            (bootstrap_budget_msat - total_capex_30d_msat).max(0)
        };
        (Tier::Active, 500, PriorityClass::Preservation, raw)
    } else if days_open >= cfg.capex_grace_days {
        (
            Tier::Bootstrap,
            250,
            PriorityClass::Growth,
            (bootstrap_budget_msat - total_capex_30d_msat).max(0),
        )
    } else {
        return ChannelCapexBudget::blocked(ch_id);
    };

    let efficiency_mult = efficiency_multiplier(ch_id, fleet_efficiency, Some(prof));
    // `int(raw_budget_msat * roi_mult * efficiency_mult)` (py 624):
    // truncate-toward-zero of a non-negative product == floor.
    let budget_msat = (raw_budget_msat as f64 * roi_mult * efficiency_mult) as i64;

    ChannelCapexBudget {
        channel_id: ch_id.to_string(),
        budget_msat,
        tier,
        tier_ppm,
        priority_class: priority,
        success_rate_30d,
        roi_multiplier: roi_mult,
    }
}

// ---------------------------------------------------------------------
// Top-level: compute_allocations (py capex_budget.py 142-330)
// ---------------------------------------------------------------------

/// Port of `CapexBudgetEngine.compute_allocations` (py capex_budget.py
/// 142-330). Pure: takes pre-fetched [`CapexEvidence`] and a
/// [`CapexConfig`] snapshot, returns the complete [`CapexAllocations`] for
/// the cycle. See the module doc comment for the purity contract.
pub fn compute_allocations(evidence: &CapexEvidence, cfg: &CapexConfig) -> CapexAllocations {
    let mut db_degraded = evidence.capex_by_channel_sats.is_none();
    let empty_capex = BTreeMap::new();
    let capex_by_channel = evidence
        .capex_by_channel_sats
        .as_ref()
        .unwrap_or(&empty_capex);

    let mut channel_budgets: BTreeMap<String, ChannelCapexBudget> = BTreeMap::new();
    let mut total_fleet_contribution_msat: i64 = 0;
    let mut hard_bleeder_count: i64 = 0;
    let mut hard_bleeder_capacity_sats: i64 = 0;
    let mut total_capacity_sats: i64 = 0;
    let mut has_depleted_earners = false;

    for (ch_id, prof) in evidence.channels.iter() {
        let capacity_sats = prof.capacity_sats;
        total_capacity_sats += capacity_sats;
        let contribution_msat = prof.total_contribution_msat;
        // Audit F1(d): fleet budgets are debited by 30d spend, so they must
        // be FUNDED by 30d revenue (exit fees only), not lifetime totals
        // (py 186-190).
        total_fleet_contribution_msat += windowed_msat(
            prof.window_30d.map(|w| w.fees_earned_30d_msat),
            prof.fees_earned_msat,
        );
        let total_capex_msat = capex_by_channel.get(ch_id).copied().unwrap_or(0) * MSAT_PER_SAT;

        let bleeder_status = evidence.bleeder_status.get(ch_id).map(|s| s.as_str());
        if bleeder_status == Some("hard") {
            hard_bleeder_count += 1;
            hard_bleeder_capacity_sats += capacity_sats;
        }

        if contribution_msat > 100 * MSAT_PER_SAT
            && (prof.classification == "underwater" || prof.classification == "stagnant_candidate")
        {
            has_depleted_earners = true;
        }

        let success_rate = evidence.success_rates.get(ch_id).copied();
        let budget = compute_channel_budget(
            ch_id,
            prof,
            total_capex_msat,
            bleeder_status,
            cfg,
            evidence.fleet_efficiency.as_ref(),
            success_rate,
        );
        channel_budgets.insert(ch_id.clone(), budget);
    }

    // Audit F4b: a single small bleeder must not flip the WHOLE fleet into
    // defensive mode (py 221-228).
    let has_hard_bleeders = hard_bleeder_count > 1
        || (total_capacity_sats > 0
            && (hard_bleeder_capacity_sats as f64 / total_capacity_sats as f64) > 0.10);

    let priority_class = detect_priority_class(
        has_hard_bleeders,
        has_depleted_earners,
        reserve_deficit(cfg, evidence.onchain_sats),
    );

    let reserve_deficit_msat = reserve_deficit(cfg, evidence.onchain_sats) * MSAT_PER_SAT;
    let mut tactical_msat = reserve_deficit_msat
        .min((total_fleet_contribution_msat as f64 * cfg.capex_tactical_rate) as i64)
        .max(0);

    if evidence.spend_summary.is_none() {
        db_degraded = true;
    }
    let empty_summary = SpendSummary::default();
    let spend_summary = evidence.spend_summary.as_ref().unwrap_or(&empty_summary);

    let mut exploration_msat = if total_fleet_contribution_msat > 0 {
        let revenue_exploration_msat =
            (total_fleet_contribution_msat as f64 * cfg.capex_exploration_rate) as i64;
        let wallet_floor_msat = wallet_exploration_floor_msat(cfg, evidence.onchain_sats);
        let exploration_msat = revenue_exploration_msat.max(wallet_floor_msat);
        apply_category_spend_remaining(exploration_msat, spend_summary, "channel_open")
    } else {
        bootstrap_wallet_exploration_budget_msat(cfg, evidence.onchain_sats, spend_summary)
    };
    tactical_msat = apply_category_spend_remaining(tactical_msat, spend_summary, "boltz");

    // CB-4 fail-closed (py 266-277).
    if db_degraded {
        exploration_msat = 0;
        tactical_msat = 0;
        for b in channel_budgets.values_mut() {
            b.budget_msat = 0;
        }
    }

    // Global envelope enforcement (msat) (py 279-302).
    let total_channel_budgets_msat: i64 = channel_budgets.values().map(|b| b.budget_msat).sum();
    let raw_total_msat = total_channel_budgets_msat + exploration_msat + tactical_msat;

    let mut envelope_msat = if cfg.capex_global_envelope_sats > 0 {
        cfg.capex_global_envelope_sats * MSAT_PER_SAT
    } else {
        raw_total_msat
    };
    if cfg.daily_budget_sats > 0 {
        envelope_msat = envelope_msat.min(cfg.daily_budget_sats * 30 * MSAT_PER_SAT);
    }
    if cfg.weekly_budget_sats > 0 {
        // `int(weekly_budget_sats * MSAT_PER_SAT * (30 / 7))` (py 293): the
        // `30 / 7` is Python float (true) division.
        let weekly_30d_msat =
            (cfg.weekly_budget_sats as f64 * MSAT_PER_SAT as f64 * (30.0 / 7.0)) as i64;
        envelope_msat = envelope_msat.min(weekly_30d_msat);
    }

    if raw_total_msat > envelope_msat && raw_total_msat > 0 {
        let scale = envelope_msat as f64 / raw_total_msat as f64;
        exploration_msat = (exploration_msat as f64 * scale) as i64;
        tactical_msat = (tactical_msat as f64 * scale) as i64;
        for b in channel_budgets.values_mut() {
            b.budget_msat = (b.budget_msat as f64 * scale) as i64;
        }
    }

    // Priority allocation tracking (msat) (py 304-327).
    let defensive_total_msat: i64 = channel_budgets
        .values()
        .filter(|b| b.priority_class == PriorityClass::Defensive)
        .map(|b| b.budget_msat)
        .sum();
    let preservation_total_msat: i64 = channel_budgets
        .values()
        .filter(|b| b.priority_class == PriorityClass::Preservation)
        .map(|b| b.budget_msat)
        .sum();

    let mut allocated_by_priority_msat = BTreeMap::new();
    allocated_by_priority_msat.insert(PriorityClass::Defensive, defensive_total_msat);
    allocated_by_priority_msat.insert(PriorityClass::Preservation, preservation_total_msat);
    allocated_by_priority_msat.insert(PriorityClass::Operational, tactical_msat);
    allocated_by_priority_msat.insert(PriorityClass::Growth, exploration_msat);

    CapexAllocations {
        priority_class,
        global_envelope_msat: envelope_msat,
        channel_budgets,
        fleet_exploration_budget_msat: exploration_msat,
        tactical_budget_msat: tactical_msat,
        allocated_by_priority_msat,
        total_fleet_contribution_msat,
        db_degraded,
    }
}

// ---------------------------------------------------------------------
// Boltz cost attribution (pure; py capex_budget.py 356-373)
// ---------------------------------------------------------------------

/// A cost split between the channel and tactical budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoltzCostSplit {
    pub channel: i64,
    pub tactical: i64,
}

/// Port of `CapexBudgetEngine.attribute_boltz_cost` (py capex_budget.py
/// 356-373): split a Boltz cost between channel and tactical budgets.
/// `channel_id: None` means a pure treasury swap — all tactical. Otherwise
/// a 50/50 split (floor division for the channel share, matching Python's
/// `//`).
pub fn attribute_boltz_cost(cost_sats: i64, channel_id: Option<&str>) -> BoltzCostSplit {
    if channel_id.is_none() {
        return BoltzCostSplit {
            channel: 0,
            tactical: cost_sats,
        };
    }
    let channel_share = cost_sats.div_euclid(2);
    let tactical_share = cost_sats - channel_share;
    BoltzCostSplit {
        channel: channel_share,
        tactical: tactical_share,
    }
}
