//! Rust port of `modules/lnplus_swaps.py` (~2,099 LOC) — LN+
//! (lightningnetwork.plus) liquidity-swap automation.
//!
//! Three collaborators, matching the Python module's own split
//! (`lnplus_swaps.py:1-11`):
//!   - [`ports::LnPlusApi`] — the LN+ v2 HTTPS API surface (`LNPlusClient`)
//!   - [`evaluator`] — pre-application gate chain (spec gates 0-9,
//!     `SwapEvaluator`)
//!   - the obligations watcher / state machine (spec gates 10-14,
//!     `SwapLifecycle`), split across [`breaker`], [`reconcile`],
//!     [`backfill`], [`open`], [`activate`], [`finalize`], [`withdrawal`],
//!     and orchestrated by [`watcher`]
//!
//! Applying to a swap is an IRREVERSIBLE COMMITMENT (48h open deadline
//! once filled). Every gate lives before `create_application`; everything
//! after only executes obligations safely — this crate mirrors that
//! ordering module-by-module.
//!
//! ## HARD RULE 1/2 compliance (kernel modules) + the wiring layer
//!
//! The KERNEL — [`activate`], [`backfill`], [`breaker`], [`config`],
//! [`db_types`], [`error`], [`evaluator`], [`finalize`], [`open`],
//! [`ports`], [`reconcile`], [`telemetry`], [`types`], [`validation`],
//! [`watcher`], [`withdrawal`] — holds no HTTP client, no SQL connection,
//! and no CLN RPC socket. Every external effect is an injected trait in
//! [`ports`]; every kernel test runs against an in-memory fake.
//!
//! [`exec_mode`], [`gated`], [`http`], [`loop_drivers`], and [`sqlite_db`]
//! are the WIRING LAYER (added after the kernel was ported and reviewed —
//! see `ENTRYPOINTS.md`'s 2026-07-27 update). [`sqlite_db`] DOES hold a
//! real `rusqlite::Connection` (a production [`ports::LnPlusDb`]); [`http`]
//! holds no live transport (a production [`ports::LnPlusApi`] generic over
//! an [`http::HttpTransport`] with no concrete implementation shipped —
//! see `REGISTER.md` §2). Every test anywhere in this crate, kernel or
//! wiring, still runs against a fake or a throwaway temp-file sqlite db —
//! never a live network call, never the production database. See
//! `ENTRYPOINTS.md` / `REGISTER.md` for what a plugin still needs to wire
//! up before any of this runs live.
//!
//! ## The five known defects this port fixes (not reproduces)
//!
//! 1. **Finalize under-reporting** — [`finalize::FinalizeOutcome`] forces
//!    the watcher (`watcher.rs` phase 5) to only count a swap as
//!    finalized when it actually was.
//! 2. **Rating idempotency substring scan** —
//!    [`finalize::rating_already_filed`] is a structural
//!    http_status+errors match, not a blob scan.
//! 3. **Terminal withdrawal responses retried as failures** —
//!    [`withdrawal::classify_delete_application_error`] classifies
//!    documented 422 terminal shapes instead of uniformly retrying.
//! 4. **Expired swaps consuming reserved budget forever** —
//!    [`open::maybe_trip_deadline_miss`] terminalizes the row when it
//!    trips the breaker on a missed deadline (see `telemetry.rs`).
//! 5. **Circuit breaker latched after backfill resolves it** —
//!    [`breaker::BreakerCause::is_reverifiable`] draws the exact
//!    re-verifiable/never-auto-clear boundary the task spec requires.

pub mod activate;
pub mod backfill;
pub mod breaker;
pub mod config;
pub mod db_types;
pub mod error;
pub mod evaluator;
pub mod exec_mode;
pub mod finalize;
pub mod gated;
pub mod http;
pub mod http_ureq;
pub mod loop_drivers;
pub mod open;
pub mod ports;
pub mod reconcile;
pub mod sqlite_db;
pub mod telemetry;
pub mod types;
pub mod validation;
pub mod watcher;
pub mod withdrawal;
