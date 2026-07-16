//! Core, dependency-light building blocks for the Rust revenue-ops port
//! (money math, canonical JSON, SCID, etc.). Populated task-by-task per
//! `docs/superpowers/plans/2026-07-16-phase1a-foundations.md`.
#![forbid(unsafe_code)]

pub mod canonical;
pub mod msat;
pub mod scid;
