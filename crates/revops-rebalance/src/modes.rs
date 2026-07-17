//! Unified rebalance-mode descriptors (port of `modules/rebalance_modes.py`,
//! refactor Phase 3D Workstream F4).
//!
//! Every rebalance — auto cycle, hot-channel protection, structural drain,
//! manual, diagnostic — prices and executes through the ONE v2 engine
//! pipeline (planner → router → executor → rebalance_history), and
//! hot-channel protection is a pair-level priority/budget modifier inside
//! the auto cycle rather than an independent subsystem. This module is the
//! spec's table expressed as DATA (priority, budget allocation, deadline,
//! authority) instead of scattered boolean kwargs; call sites route their
//! engine kwargs through it so a mode's semantics live in exactly one
//! place.
//!
//! DOCUMENTED SPEC CONTRADICTION (preserved verbatim per the refactor
//! protocol, from `rebalance_modes.py:18-23`'s module docstring): the
//! spec's F4 table lists Diagnostic as "No spend". The repo's diagnostic
//! mode (defibrillation shock) is deliberately a BOUNDED spend — a small
//! real probe fee under `diagnostic_rebalance_max_fee_sats`, reserved on
//! the unified rail (P4-020), because a free probe cannot prove
//! routability. **Diagnostic is a BOUNDED spend (reserved on rail), not
//! "no spend".** Repo reality wins; recorded in
//! `docs/refactor/phase0/README.md`.
//!
//! ## `deadline_secs` is a Rust-only addition
//!
//! Python's `deadline` field (`rebalance_modes.py:41-42`) is a
//! documentation-only string class — `"immediate"` / `"short"` /
//! `"normal"` / `"long"` / `"none"` — annotated "Typical deadline class
//! (documentation + future arbiter input)" and never consumed numerically
//! anywhere in the current codebase (grepped: no reads of `.deadline`
//! outside this module). The frozen Rust interface names this field
//! `deadline_secs: i64`, so this scaffold assigns a canonical seconds value
//! per class (below) for the future arbiter to reason about concretely.
//! This mapping is NOT byte-parity-verified against Python — there is
//! nothing in Python to verify it against — and should be revisited
//! against real arbiter requirements when one lands. [`deadline_secs_for_class`]
//! is exposed so the fixture-parity test can check self-consistency against
//! the Python string classes the fixture actually dumps.

/// `"immediate"` (manual): no deadline pressure, operator-explicit.
pub const DEADLINE_IMMEDIATE_SECS: i64 = 0;
/// `"short"` (hot_protection): revenue-protection urgency window.
pub const DEADLINE_SHORT_SECS: i64 = 900;
/// `"normal"` (normal/auto cycle): standard maintenance cadence.
pub const DEADLINE_NORMAL_SECS: i64 = 3600;
/// `"long"` (structural_drain): low-urgency structural rebalancing.
pub const DEADLINE_LONG_SECS: i64 = 21_600;
/// `"none"` (diagnostic): no deadline pressure (bounded spend, not time-boxed).
pub const DEADLINE_NONE_SECS: i64 = i64::MAX;

/// Rust-side numeric encoding of Python's `deadline` string class. See the
/// module doc comment.
pub fn deadline_secs_for_class(class: &str) -> i64 {
    match class {
        "immediate" => DEADLINE_IMMEDIATE_SECS,
        "short" => DEADLINE_SHORT_SECS,
        "normal" => DEADLINE_NORMAL_SECS,
        "long" => DEADLINE_LONG_SECS,
        "none" => DEADLINE_NONE_SECS,
        other => panic!("unknown deadline class: {other}"),
    }
}

/// Port of `rebalance_modes.py:31-45` (`RebalanceMode` frozen dataclass).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModeRow {
    /// Requested priority for arbitration (J3 ladder; operator=100).
    pub priority: i64,
    /// Budget allocation per the spec table.
    pub budget_bucket: &'static str,
    /// Atomic reservation on the unified rail.
    pub reserve_on_rail: bool,
    /// Engine records settled fee into rebalance_costs.
    pub account_costs: bool,
    /// Rust-only numeric deadline encoding; see module doc comment.
    pub deadline_secs: i64,
    /// Who owns accounting when not on the rail.
    pub accounting_owner: &'static str,
}

/// Port of `rebalance_modes.py:52-56` (`MODES["normal"]`): auto cycle.
pub static NORMAL: ModeRow = ModeRow {
    priority: 50,
    budget_bucket: "maintenance",
    reserve_on_rail: true,
    account_costs: true,
    deadline_secs: DEADLINE_NORMAL_SECS,
    accounting_owner: "engine",
};

/// Port of `rebalance_modes.py:57-62` (`MODES["hot_protection"]`).
pub static HOT_PROTECTION: ModeRow = ModeRow {
    priority: 90,
    budget_bucket: "revenue_protection",
    reserve_on_rail: true,
    account_costs: true,
    deadline_secs: DEADLINE_SHORT_SECS,
    accounting_owner: "engine",
};

/// Port of `rebalance_modes.py:63-67` (`MODES["structural_drain"]`).
pub static STRUCTURAL_DRAIN: ModeRow = ModeRow {
    priority: 20,
    budget_bucket: "structural",
    reserve_on_rail: true,
    account_costs: true,
    deadline_secs: DEADLINE_LONG_SECS,
    accounting_owner: "engine",
};

/// Port of `rebalance_modes.py:70-74` (`MODES["manual"]`): operator-explicit,
/// P4-020 engine reservation deliberately skipped.
pub static MANUAL: ModeRow = ModeRow {
    priority: 100,
    budget_bucket: "operator_explicit",
    reserve_on_rail: false,
    account_costs: false,
    deadline_secs: DEADLINE_IMMEDIATE_SECS,
    accounting_owner: "caller",
};

/// Port of `rebalance_modes.py:78-82` (`MODES["diagnostic"]`): bounded
/// diagnostic spend, atomically reserved (P4-020/P4-025). See the
/// diagnostic spec-contradiction note in the module doc comment.
pub static DIAGNOSTIC: ModeRow = ModeRow {
    priority: 10,
    budget_bucket: "diagnostic",
    reserve_on_rail: true,
    account_costs: true,
    deadline_secs: DEADLINE_NONE_SECS,
    accounting_owner: "engine",
};

/// Port of `MODES` (`rebalance_modes.py:47-83`) lookup by name.
pub fn mode(name: &str) -> Option<&'static ModeRow> {
    match name {
        "normal" => Some(&NORMAL),
        "hot_protection" => Some(&HOT_PROTECTION),
        "structural_drain" => Some(&STRUCTURAL_DRAIN),
        "manual" => Some(&MANUAL),
        "diagnostic" => Some(&DIAGNOSTIC),
        _ => None,
    }
}

/// Port of `rebalance_modes.py:86-92` (`engine_kwargs`): the engine
/// `execute_candidate` kwargs a mode implies. Manual (priority 100) skips
/// reservation; caller accounts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineKwargs {
    pub reserve_budget: bool,
    pub account_costs: bool,
}

/// Panics on an unknown mode name, mirroring Python's `MODES[mode_name]`
/// dict-index `KeyError`.
pub fn engine_kwargs(mode_name: &str) -> EngineKwargs {
    let m = mode(mode_name).unwrap_or_else(|| panic!("unknown rebalance mode: {mode_name}"));
    EngineKwargs {
        reserve_budget: m.reserve_on_rail,
        account_costs: m.account_costs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_skips_reservation_and_accounting() {
        let kw = engine_kwargs("manual");
        assert!(!kw.reserve_budget);
        assert!(!kw.account_costs);
        assert_eq!(mode("manual").unwrap().priority, 100);
    }

    #[test]
    fn diagnostic_is_bounded_spend_not_no_spend() {
        // The documented spec contradiction: diagnostic reserves on rail.
        let d = mode("diagnostic").unwrap();
        assert!(d.reserve_on_rail);
        assert!(d.account_costs);
    }

    #[test]
    fn unknown_mode_returns_none() {
        assert!(mode("not_a_real_mode").is_none());
    }

    #[test]
    #[should_panic(expected = "unknown rebalance mode")]
    fn engine_kwargs_panics_on_unknown_mode() {
        engine_kwargs("not_a_real_mode");
    }
}
