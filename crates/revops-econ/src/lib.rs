//! Governed economic core (Rust port of the Python `econ_*` / `governor_facade`
//! / `cycle_context` / `reason_codes` layer). Populated task-by-task per
//! `docs/superpowers/plans/2026-07-16-phase2-econ-core.md`.
//!
//! Every module for the whole phase is pre-declared here (Task 1, Wave 0) so
//! that Waves 1-4 — implemented as parallel, isolated tasks/worktrees — never
//! need to touch this file again. Modules land as stub files with doc
//! comments only until their owning task replaces them.
#![forbid(unsafe_code)]

pub mod arbiter;
pub mod context;
pub mod cycle;
pub mod ev;
pub mod governor;
pub mod intents;
pub mod ledger;
pub mod pyfloat;
pub mod reason;
pub mod reconcile;
pub mod shadow;
pub mod snapshot;
pub mod types;
