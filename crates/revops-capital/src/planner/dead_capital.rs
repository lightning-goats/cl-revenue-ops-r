//! Port of the dead-capital stage-transition machine inside
//! `CapacityPlanner._build_dead_capital_loser` (py
//! `modules/capacity_planner.py` 1241-1382): `none -> fee_reduction ->
//! defibrillation -> close`, with per-stage timeouts (24h local / 48h
//! remote) and two demotion guards — a protected channel or a channel with
//! no EXECUTED defibrillation attempt can never advance to (or stay at)
//! `close`.
//!
//! # What is and is not pure here
//!
//! Python's `_build_dead_capital_loser` does three things this module
//! deliberately separates:
//!  1. **the stage transition decision** (this module) — pure given the
//!     current stage row, `now`, `opener`, and two pieces of evidence
//!     (`close_protection` from `protection_service.close_protection_reason`
//!     and `defib_attempted` from `_dead_capital_defib_attempted`);
//!  2. **recording the FEE_REDUCE delegation** (py
//!     `_record_fee_reduce_delegation`, 1191-1239) — a DB write + log call
//!     the caller must perform and report back via
//!     [`DeadCapitalInput::fee_reduce_delegation_recorded`] BEFORE calling
//!     this function, because Python's `persist_stage` is set on the
//!     `none -> fee_reduction` transition ONLY if that recording succeeded
//!     (py 1285-1286) — the ACTION is `FEE_REDUCE` either way, only the DB
//!     persistence of the stage row is gated;
//!  3. **persisting the new stage row** (py `db.upsert_dead_capital_stage`,
//!     1347-1351) — the caller does this when [`DeadCapitalDecision::persist`]
//!     is `true`.

/// `dead_capital_stage` (py `ChannelCapitalEfficiency.dead_capital_stage` /
/// the `dead_capital_stages` DB table's `stage` column). `Unstaged`
/// mirrors Python's `"none"` string default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeadCapitalStage {
    #[default]
    Unstaged,
    FeeReduction,
    Defibrillation,
    Close,
}

impl DeadCapitalStage {
    pub fn as_str(&self) -> &'static str {
        match self {
            DeadCapitalStage::Unstaged => "none",
            DeadCapitalStage::FeeReduction => "fee_reduction",
            DeadCapitalStage::Defibrillation => "defibrillation",
            DeadCapitalStage::Close => "close",
        }
    }

    /// Any value that is not one of the three known stage strings is
    /// treated as `"none"`, matching Python's `str(... or "none")` default
    /// (py 1267-1271).
    pub fn parse(s: &str) -> Self {
        match s {
            "fee_reduction" => DeadCapitalStage::FeeReduction,
            "defibrillation" => DeadCapitalStage::Defibrillation,
            "close" => DeadCapitalStage::Close,
            _ => DeadCapitalStage::Unstaged,
        }
    }
}

/// The planner action a dead-capital-staged loser should carry (py's
/// `action` local, values `"FEE_REDUCE"` / `"DEFIBRILLATE"` / `"CLOSE"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadCapitalAction {
    FeeReduce,
    Defibrillate,
    Close,
}

impl DeadCapitalAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            DeadCapitalAction::FeeReduce => "FEE_REDUCE",
            DeadCapitalAction::Defibrillate => "DEFIBRILLATE",
            DeadCapitalAction::Close => "CLOSE",
        }
    }
}

/// The persisted `dead_capital_stages` row for one channel (py
/// `stage_row = dead_capital_stages.get(scid, {})`, 1266, 1272).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeadCapitalStageRow {
    pub stage: DeadCapitalStage,
    /// Unix seconds; `0` mirrors Python's falsy/missing `entered_at` (the
    /// timeout check is gated on `entered_at` being truthy, py 1287, 1294).
    pub entered_at: i64,
}

/// Every input `advance_dead_capital_stage` needs (py 1241-1249 signature
/// plus the two evidence calls it makes internally, hoisted to the
/// caller — see the module doc comment).
#[derive(Debug, Clone, Copy)]
pub struct DeadCapitalInput<'a> {
    pub current: DeadCapitalStageRow,
    /// `"local"` or `"remote"` (py `getattr(prof, "opener", "local")`,
    /// 1264); anything other than `"remote"` uses the local (24h) timeout.
    pub opener: &'a str,
    /// `_close_protection_reason(...)` result (py 1258-1260): `Some(reason)`
    /// when a protective gate (inbound gateway, sourced-fee contributor,
    /// revenue route, low-confidence Kalman) forbids staging into `close`.
    pub close_protection: Option<&'a str>,
    /// `_dead_capital_defib_attempted(scid)` (py 1300, 1313): at least one
    /// diagnostic rebalance EXECUTED in the last 14 days.
    pub defib_attempted: bool,
    /// Whether `_record_fee_reduce_delegation` succeeded THIS call (only
    /// consulted on the `none -> fee_reduction` transition, py 1285-1286).
    pub fee_reduce_delegation_recorded: bool,
    pub now: i64,
}

/// The transition decision (py's local `action`/`stage`/`entered_at` plus
/// the `persist_stage is not None` / `close_blocked` / `defib_gate_hold`
/// flags, 1273-1345).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeadCapitalDecision {
    pub action: DeadCapitalAction,
    pub stage: DeadCapitalStage,
    pub entered_at: i64,
    /// `true` iff the caller should call `upsert_dead_capital_stage` (py
    /// `persist_stage is not None`).
    pub persist: bool,
    /// `true` when a `close` stage timed out but was demoted back to
    /// `defibrillation` because of an active protection (py 1295-1299,
    /// 1313-1322 "or" branch when `close_protection` is set).
    pub close_blocked: bool,
    /// `true` when a `close` transition was held at `defibrillation`
    /// because no diagnostic rebalance has EXECUTED yet (py 1300-1304,
    /// 1313-1322 "or" branch when `close_protection` is `None`).
    pub defib_gate_hold: bool,
}

/// Port of the stage-transition logic inside `_build_dead_capital_loser`
/// (py `modules/capacity_planner.py` 1273-1345).
pub fn advance_dead_capital_stage(input: &DeadCapitalInput) -> DeadCapitalDecision {
    let stage_timeout: i64 = if input.opener == "remote" {
        48 * 3600
    } else {
        24 * 3600
    };
    let now = input.now;
    let cur = input.current;

    let timed_out = cur.entered_at != 0 && now - cur.entered_at >= stage_timeout;

    match cur.stage {
        DeadCapitalStage::Unstaged => DeadCapitalDecision {
            action: DeadCapitalAction::FeeReduce,
            stage: DeadCapitalStage::FeeReduction,
            entered_at: now,
            persist: input.fee_reduce_delegation_recorded,
            close_blocked: false,
            defib_gate_hold: false,
        },
        DeadCapitalStage::FeeReduction if timed_out => DeadCapitalDecision {
            action: DeadCapitalAction::Defibrillate,
            stage: DeadCapitalStage::Defibrillation,
            entered_at: now,
            persist: true,
            close_blocked: false,
            defib_gate_hold: false,
        },
        DeadCapitalStage::FeeReduction => DeadCapitalDecision {
            action: DeadCapitalAction::FeeReduce,
            stage: DeadCapitalStage::FeeReduction,
            entered_at: cur.entered_at,
            persist: false,
            close_blocked: false,
            defib_gate_hold: false,
        },
        DeadCapitalStage::Defibrillation if timed_out => {
            if input.close_protection.is_some() {
                DeadCapitalDecision {
                    action: DeadCapitalAction::Defibrillate,
                    stage: DeadCapitalStage::Defibrillation,
                    entered_at: cur.entered_at,
                    persist: false,
                    close_blocked: true,
                    defib_gate_hold: false,
                }
            } else if !input.defib_attempted {
                DeadCapitalDecision {
                    action: DeadCapitalAction::Defibrillate,
                    stage: DeadCapitalStage::Defibrillation,
                    entered_at: cur.entered_at,
                    persist: false,
                    close_blocked: false,
                    defib_gate_hold: true,
                }
            } else {
                DeadCapitalDecision {
                    action: DeadCapitalAction::Close,
                    stage: DeadCapitalStage::Close,
                    entered_at: now,
                    persist: true,
                    close_blocked: false,
                    defib_gate_hold: false,
                }
            }
        }
        DeadCapitalStage::Defibrillation => DeadCapitalDecision {
            action: DeadCapitalAction::Defibrillate,
            stage: DeadCapitalStage::Defibrillation,
            entered_at: cur.entered_at,
            persist: false,
            close_blocked: false,
            defib_gate_hold: false,
        },
        // py's final `else:` (1312-1324) handles "close" and any unknown
        // stage identically.
        DeadCapitalStage::Close => {
            if input.close_protection.is_some() || !input.defib_attempted {
                DeadCapitalDecision {
                    action: DeadCapitalAction::Defibrillate,
                    stage: DeadCapitalStage::Defibrillation,
                    entered_at: now,
                    persist: true,
                    close_blocked: input.close_protection.is_some(),
                    defib_gate_hold: input.close_protection.is_none(),
                }
            } else {
                DeadCapitalDecision {
                    action: DeadCapitalAction::Close,
                    stage: DeadCapitalStage::Close,
                    entered_at: cur.entered_at,
                    persist: false,
                    close_blocked: false,
                    defib_gate_hold: false,
                }
            }
        }
    }
}
