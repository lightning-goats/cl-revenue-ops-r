//! THE BUDGET RAIL — reserve/spend/release lifecycle over the production
//! schema's `budget_reservations` + `spend_reservations` + `spend_events`
//! tables (port of `modules/database.py`'s `_reserve_budget_atomic` /
//! `reserve_spend` / release / settle / sweep family).
//!
//! Stub — implemented in Task 6 (Wave 1) of
//! `docs/superpowers/plans/2026-07-17-phase3-analytics-budget.md`.
//!
//! # PRODUCTION-WRITE CONSTRAINT (read this twice)
//!
//! This phase writes the PRODUCTION SCHEMA SHAPE for the first time in Rust
//! — but it NEVER writes the production database. Operator ruling (design
//! spec constraint 2): the Rust plugin never writes lnnode's
//! `revenue_ops.db` and never holds action authority until an explicit
//! per-subsystem flag cutover. Therefore, in this phase and until cutover:
//!
//! - Every `revops-db::budget` write path operates ONLY on (a) the plugin's
//!   OWN parallel DB file (the `owner.rs` pattern — plugin-created, never
//!   the production path), or (b) throwaway COPIES of fixtures
//!   (`fixtures/fixture.db`, tempdir copies) in tests.
//! - No task in this plan opens the production DB read-write, adds a
//!   production-path default, or wires the rail to live RPC authority.
//!   Wiring + cutover rehearsal on a DB copy is a later, separately-gated
//!   step ("rehearse each cutover on a DB copy first" — spec risk
//!   register).
//! - Test hygiene: every write-path test constructs its DB via
//!   `tempfile::TempDir` + either the exact Python DDL or a copy of
//!   `fixtures/fixture.db`. A test that takes a DB path from an env var
//!   must refuse paths outside the tempdir unless `#[ignore]`d and
//!   explicitly named `prod_copy`.
