//! Pure decision kernels ported from `CapacityPlanner` (py
//! `modules/capacity_planner.py`, ~4,200 LOC), including the `execute_cycle`
//! orchestration itself ([`cycle::plan_cycle`]) and the five candidate
//! discovery strategies ([`discovery`], [`demand_flow`]). See each
//! submodule's doc comment for exact `py <file>:<line>` provenance, and
//! `crates/revops-capital/ENTRYPOINTS.md` for what is and is not ported —
//! every `_execute_*` RPC call site remains Python-owned, and this crate is
//! structurally incapable of calling them (no RPC client type anywhere in
//! the crate).

pub mod candidate_score;
pub mod close_fee;
pub mod cycle;
pub mod dead_capital;
pub mod dedup;
pub mod demand_flow;
pub mod discovery;
pub mod ev;
pub mod gates;
pub mod losers;
pub mod portfolio_gate;
pub mod pyround;
pub mod recycle;
pub mod scoring;
pub mod sizing;
pub mod winners;
