//! Rust port of the capital-allocation subsystem of `cl_revenue_ops`
//! (`modules/capex_budget.py` — `CapexBudgetEngine`, and a pure-kernel
//! subset of `modules/capacity_planner.py` — `CapacityPlanner`), ported per
//! `docs/port/port-map.json` lens "Capital allocation subsystem".
//!
//! # Scope
//!
//! [`capex`] ports `CapexBudgetEngine` (py `modules/capex_budget.py`,
//! 789 LOC) close to completely: it is documented in the port map as a
//! "pure-calculation" engine (no CLN RPC calls) that reads from a
//! profitability cache and spend history and computes budgets. The Rust
//! port keeps that purity contract literally: [`capex::compute_allocations`]
//! takes typed, already-fetched evidence and a config snapshot, and returns
//! [`capex::CapexAllocations`] — no trait objects doing I/O inside the
//! decision path. [`boltz_reservation`] wraps the Boltz swap-budget
//! reservation lifecycle (reserve -> settle -> release, P4-019 ordering)
//! over the EXISTING, already-audited `revops_db::budget::BudgetDb` rail
//! rather than reinventing money-safety code.
//!
//! [`planner`] ports a curated, pure subset of `CapacityPlanner`
//! (py `modules/capacity_planner.py`, ~4,200 LOC) — the decision gates and
//! EV/ROI math that are pure functions of already-fetched evidence. It does
//! **not** port `execute_cycle`, the five discovery strategies, or any of
//! the `_execute_*` methods that perform CLN RPC calls (fundchannel/close/
//! rebalance): those remain Python-owned. See
//! `crates/revops-capital/ENTRYPOINTS.md` for the complete list of what is
//! and is not wired, and what a caller integrating this crate must supply.
//!
//! Not ported at all in this pass: `BoltzCliManager` (py
//! `modules/boltz_manager.py`, ~2,670 LOC), `BoltzAutoCycle` (py
//! `cl-revenue-ops.py`, ~1,400 LOC), `LNPlusSwapAutomation` (py
//! `modules/lnplus_swaps.py`, ~2,099 LOC). These are named as separate
//! components in the port-map lens and are large enough to be their own
//! porting projects; see ENTRYPOINTS.md.
#![forbid(unsafe_code)]

pub mod boltz_reservation;
pub mod capex;
pub mod planner;
