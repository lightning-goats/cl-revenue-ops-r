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
//! ## HARD RULE 1/2 compliance
//!
//! No HTTP client, no SQL connection, and no CLN RPC socket live in this
//! crate. Every external effect is an injected trait in [`ports`]; every
//! test in `tests/` runs against an in-memory fake. See
//! `ENTRYPOINTS.md` for what a plugin needs to wire up before any of this
//! runs live.
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
pub mod finalize;
pub mod open;
pub mod ports;
pub mod reconcile;
pub mod telemetry;
pub mod types;
pub mod validation;
pub mod watcher;
pub mod withdrawal;
