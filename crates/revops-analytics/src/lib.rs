//! Pure analysis layer (Rust port of the Python classification /
//! flow-analysis / profitability / policy / protection / growth-budget /
//! datastore-telemetry modules). Populated task-by-task per
//! `docs/superpowers/plans/2026-07-17-phase3-analytics-budget.md`.
//!
//! Every module for the whole phase is pre-declared here (Task 1, Wave 0) so
//! that Waves 1-2 — implemented as parallel, isolated tasks/worktrees — never
//! need to touch this file again. Modules land as stub files with doc
//! comments only until their owning task replaces them.
#![forbid(unsafe_code)]

pub mod classification;
pub mod demand_flow;
pub mod flow;
pub mod growth;
pub mod kalman;
pub mod policy;
pub mod profitability;
pub mod protection;
pub mod telemetry;
