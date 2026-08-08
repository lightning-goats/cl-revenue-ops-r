//! Reporting-only capital-allocation subsystem for `cl_revenue_ops`.
//!
//! [`capex`] ports the pure `CapexBudgetEngine` calculation from
//! `modules/capex_budget.py`: it consumes already-fetched profitability and
//! spend evidence and returns typed budget allocations. Channel lifecycle
//! planning and swap authority were intentionally retired in v3, so this
//! crate contains no executor, scheduler, subprocess, HTTP client, or CLN RPC
//! adapter.
#![forbid(unsafe_code)]

pub mod capex;
