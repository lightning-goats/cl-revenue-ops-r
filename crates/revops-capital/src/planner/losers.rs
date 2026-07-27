//! Port of `_identify_losers` (py `modules/capacity_planner.py` 901-1129)
//! and the non-transition-machine parts of `_build_dead_capital_loser` (py
//! 1241-1382, gate + Loser-record assembly — the stage-transition math
//! itself is [`super::dead_capital::advance_dead_capital_stage`]).
//!
//! Evidence hoisted to the caller, all real DB/analyzer calls Python makes
//! per channel:
//! - `get_channel_rebalance_success_rate` -> [`RebalanceSuccessStats`]
//! - `get_diagnostic_rebalance_stats(...).attempt_count` ->
//!   [`LoserChannelEvidence::diagnostic_attempt_count`]
//! - the bleeder-list membership check (`identify_bleeders_v2`) ->
//!   [`LoserChannelEvidence::is_hard_bleeder`]
//! - `_close_protection_reason` (now `revops_analytics::protection`,
//!   see `crates/revops-capital/ENTRYPOINTS.md`) ->
//!   [`LoserChannelEvidence::close_protection_reason`] — consulted by BOTH
//!   the dead-capital gate and the regular-path skip gate, matching
//!   Python's two call sites with identical arguments (same `scid`,
//!   `prof`, `flow_metrics`, `route_pair_channels` — hoisted once)
//! - `_check_defib_allowed`'s `"rebalance_mode=" in str(reason)` detection
//!   -> [`LoserChannelEvidence::defib_policy_blocked`]
//! - `get_peer_uptime_percent` -> [`LoserChannelEvidence::uptime_pct`]
//! - capital-efficiency's `is_dead_capital`/`dead_capital_stage` ->
//!   [`DeadCapitalGateEvidence`]

use super::dead_capital::{
    self, advance_dead_capital_stage, DeadCapitalInput, DeadCapitalStageRow,
};
use super::pyround::py_round;
use super::winners::RebalanceSuccessStats;

/// The `action` a loser carries (py's `"FEE_REDUCE"|"DEFIBRILLATE"|"CLOSE"`
/// strings) — re-exported from [`dead_capital`] since both modules produce
/// exactly this vocabulary (the regular, non-dead-capital path never
/// produces `FeeReduce`).
pub use super::dead_capital::DeadCapitalAction as LoserAction;

/// The flow-analyzer fields the regular (non-dead-capital) loser path
/// reads (py 1014, 1030-1034, 1056-1060). `None` mirrors py's `flow_metrics
/// is None`.
#[derive(Debug, Clone, Copy)]
pub struct LoserFlowEvidence {
    pub flow_ratio: f64,
    /// py `flow_metrics.capacity` (1030) — read separately from
    /// `prof.capacity_sats`; pass the same evidence Python's flow analyzer
    /// would if the two happen to differ.
    pub capacity: i64,
    pub daily_volume: f64,
    pub kalman_regime_change: bool,
}

/// Capital-efficiency evidence gating the dead-capital branch (py
/// 1251-1272). Present only when a capital-efficiency snapshot for this
/// channel exists (py `channel_efficiency is not None`); when
/// `is_dead_capital` is `false` the caller should still populate this (or
/// omit it) — either way [`identify_losers`] falls through to the regular
/// path, matching py's `if channel_efficiency is None or not ...
/// is_dead_capital: return None`.
#[derive(Debug, Clone, Copy)]
pub struct DeadCapitalGateEvidence {
    pub is_dead_capital: bool,
    /// py `getattr(channel_efficiency, "dead_capital_stage", "none")`,
    /// pre-parsed. `None` mirrors an unset/falsy (`None`/`""`) attribute —
    /// the resolved current stage then falls back to `stage_row.stage`.
    pub channel_efficiency_stage: Option<dead_capital::DeadCapitalStage>,
    /// The persisted `dead_capital_stages` row (py `dead_capital_stages.get(scid,
    /// {})`); `DeadCapitalStageRow::default()` (Unstaged/0) for "no row".
    pub stage_row: DeadCapitalStageRow,
    pub defib_attempted: bool,
    pub fee_reduce_delegation_recorded: bool,
    pub now: i64,
}

/// Every input `_identify_losers` needs for one channel.
#[derive(Debug, Clone)]
pub struct LoserChannelEvidence {
    pub scid: String,
    pub peer_id: String,
    pub capacity_sats: i64,
    pub roi_percent: f64,
    pub marginal_roi_percent: f64,
    pub marginal_profit_30d_sats: i64,
    /// py `prof.classification.value` — already the lowercase string
    /// (`"zombie"`, `"underwater"`, `"profitable"`, ...). Compared against
    /// the `"zombie"`/`"underwater"` literals for the fire-sale gate,
    /// matching Python's enum-identity comparison (both are 1:1 with the
    /// `.value` string in the source enum).
    pub classification: String,
    pub days_open: i64,
    /// `"local"` or `"remote"` (py `getattr(prof, "opener", "local")`).
    pub opener: String,
    pub flow: Option<LoserFlowEvidence>,
    pub rebalance_success: Option<RebalanceSuccessStats>,
    pub diagnostic_attempt_count: i64,
    pub is_hard_bleeder: bool,
    /// py: `_check_defib_allowed(peer_id)` returned `(False, reason)` with
    /// `"rebalance_mode="` in `reason` (1087-1094) — the deadlock-avoidance
    /// waiver, not "any policy denial".
    pub defib_policy_blocked: bool,
    pub close_protection_reason: Option<String>,
    pub uptime_pct: Option<f64>,
    pub estimated_closure_cost_sats: i64,
    pub dead_capital: Option<DeadCapitalGateEvidence>,
}

/// A classified loser (py's dict, 1109-1127 / 1363-1382).
#[derive(Debug, Clone, PartialEq)]
pub struct Loser {
    pub scid: String,
    pub peer_id: String,
    pub reason: String,
    pub roi: f64,
    pub marginal_roi: f64,
    pub classification: String,
    pub capacity: i64,
    pub estimated_closure_cost_sats: i64,
    pub rebal_difficulty: f64,
    pub opener: String,
    pub action: LoserAction,
    pub is_hard_bleeder: bool,
    pub uptime_pct: Option<f64>,
    pub regime_change: bool,
    pub is_fire_sale: bool,
    pub marginal_profit_30d_sats: i64,
    /// `Some(..)` only for a dead-capital-staged loser (py 1380).
    pub dead_capital_stage: Option<String>,
    /// `Some(..)` only when a dead-capital `close` staging was blocked by
    /// an active protection (py 1381: `close_protection if close_blocked
    /// else None`).
    pub close_protection: Option<String>,
}

fn build_dead_capital_loser(
    scid_display: &str,
    ev: &LoserChannelEvidence,
    dc: &DeadCapitalGateEvidence,
) -> Loser {
    let current = DeadCapitalStageRow {
        stage: dc.channel_efficiency_stage.unwrap_or(dc.stage_row.stage),
        entered_at: dc.stage_row.entered_at,
    };
    let input = DeadCapitalInput {
        current,
        opener: &ev.opener,
        close_protection: ev.close_protection_reason.as_deref(),
        defib_attempted: dc.defib_attempted,
        fee_reduce_delegation_recorded: dc.fee_reduce_delegation_recorded,
        now: dc.now,
    };
    let decision = advance_dead_capital_stage(&input);

    Loser {
        scid: scid_display.to_string(),
        peer_id: ev.peer_id.clone(),
        reason: "DEAD_CAPITAL".to_string(),
        roi: py_round(ev.roi_percent, 2),
        marginal_roi: py_round(ev.marginal_roi_percent, 2),
        classification: ev.classification.clone(),
        capacity: ev.capacity_sats,
        estimated_closure_cost_sats: ev.estimated_closure_cost_sats,
        rebal_difficulty: 0.0,
        opener: ev.opener.clone(),
        action: decision.action,
        is_hard_bleeder: false,
        uptime_pct: ev.uptime_pct.map(|u| py_round(u, 1)),
        regime_change: false,
        is_fire_sale: false,
        marginal_profit_30d_sats: ev.marginal_profit_30d_sats,
        dead_capital_stage: Some(decision.stage.as_str().to_string()),
        close_protection: if decision.close_blocked {
            ev.close_protection_reason.clone()
        } else {
            None
        },
    }
}

/// Port of `_identify_losers` (py 901-1129), composed with the dead-capital
/// gate (py 1251-1382, via [`build_dead_capital_loser`]).
pub fn identify_losers(channels: &[LoserChannelEvidence]) -> Vec<Loser> {
    let mut losers = Vec::new();

    for ev in channels {
        let scid_display = ev.scid.replace(':', "x");

        if let Some(dc) = &ev.dead_capital {
            if dc.is_dead_capital {
                losers.push(build_dead_capital_loser(&scid_display, ev, dc));
                continue;
            }
        }

        let rebal_difficulty = match &ev.rebalance_success {
            Some(s) if s.total >= 3 => 1.0 - s.success_rate,
            _ => 0.0,
        };

        // py 997-998: protective closure gates skip the whole channel.
        if ev.close_protection_reason.is_some() {
            continue;
        }

        let is_hard_bleeder = ev.is_hard_bleeder;

        let mut is_fire_sale = false;
        let mut fire_sale_reason: Option<&'static str> = None;
        if ev.days_open > 90 && ev.flow.is_some() {
            if ev.classification == "zombie" {
                is_fire_sale = true;
                fire_sale_reason = Some("ZOMBIE");
            } else if ev.classification == "underwater" && ev.marginal_roi_percent < -50.0 {
                is_fire_sale = true;
                fire_sale_reason = Some("FIRE SALE");
            }
        }

        let mut is_stagnant = false;
        if let Some(flow) = &ev.flow {
            let cap = flow.capacity.max(0);
            let turnover = if cap > 0 {
                flow.daily_volume / cap as f64
            } else {
                0.0
            };
            let is_balanced = flow.flow_ratio.abs() < 0.2;
            if is_balanced && turnover < 0.0015 && ev.marginal_roi_percent < 10.0 {
                is_stagnant = true;
            }
        }

        if rebal_difficulty > 0.7 && !is_fire_sale && is_stagnant {
            is_fire_sale = true;
            fire_sale_reason = Some("STAGNANT+HARD_REBAL");
        }

        // py 1044-1050: remote-opened "free" capacity gets a much higher
        // closure bar — skip the whole channel unless deeply underwater.
        if ev.opener == "remote" && is_fire_sale && !is_stagnant && ev.marginal_roi_percent > -75.0
        {
            continue;
        }

        let regime_change = ev.flow.map(|f| f.kalman_regime_change).unwrap_or(false);

        if !(is_fire_sale || is_stagnant) {
            continue;
        }

        let mut reason = fire_sale_reason.unwrap_or("STAGNANT").to_string();
        let action;
        if is_hard_bleeder || ev.diagnostic_attempt_count >= 2 || ev.defib_policy_blocked {
            if regime_change && !is_hard_bleeder && !ev.defib_policy_blocked {
                action = LoserAction::Defibrillate;
                reason = format!("{reason} (REGIME CHANGE)");
            } else {
                action = LoserAction::Close;
            }
        } else {
            action = LoserAction::Defibrillate;
            reason = format!("{reason} (NEEDS DEFIBRILLATOR)");
        }

        losers.push(Loser {
            scid: scid_display,
            peer_id: ev.peer_id.clone(),
            reason,
            roi: py_round(ev.roi_percent, 2),
            marginal_roi: py_round(ev.marginal_roi_percent, 2),
            classification: ev.classification.clone(),
            capacity: ev.capacity_sats,
            estimated_closure_cost_sats: ev.estimated_closure_cost_sats,
            rebal_difficulty: py_round(rebal_difficulty, 2),
            opener: ev.opener.clone(),
            action,
            is_hard_bleeder,
            uptime_pct: ev.uptime_pct.map(|u| py_round(u, 1)),
            regime_change,
            is_fire_sale,
            marginal_profit_30d_sats: ev.marginal_profit_30d_sats,
            dead_capital_stage: None,
            close_protection: None,
        });
    }

    losers
}
