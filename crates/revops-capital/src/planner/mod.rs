//! Pure decision kernels ported from `CapacityPlanner` (py
//! `modules/capacity_planner.py`, ~4,200 LOC). See each submodule's doc
//! comment for exact `py <file>:<line>` provenance, and
//! `crates/revops-capital/ENTRYPOINTS.md` for what is and is not ported —
//! in particular, `execute_cycle`, the five discovery strategies, and every
//! `_execute_*` RPC call site remain Python-owned.

pub mod close_fee;
pub mod dead_capital;
pub mod ev;
pub mod gates;
pub mod portfolio_gate;
pub mod scoring;
