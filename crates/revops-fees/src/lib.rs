//! Rust port of `modules/fee_controller.py` + `modules/admission_policy.py`
//! (cl_revenue_ops v2.18.1, branch `port`): discounted Gaussian Thompson
//! sampling, the PI inventory controller, evidence-backed floors, market
//! intelligence, the htlcmax admission valve, and the frozen ADR-001 rail
//! order — dry-run decision journal only (no `setchannel` until cutover).
//!
//! Parity discipline (Phase 4 Global Constraints): every float-producing
//! function is pinned by fixtures generated from the REAL Python
//! (`tools/port/gen_fees_fixtures.py` in the port worktree) and compared as
//! CPython `repr` strings via `revops_econ::pyfloat::py_repr` — bit-for-bit,
//! never epsilon. Clock (`now: i64`) and RNG (`&mut pyrand::PyRandom`) are
//! injected everywhere.

#![forbid(unsafe_code)]

pub mod admission;
pub mod cycle;
pub mod drain;
pub mod execution;
pub mod floors;
pub mod journal;
pub mod market;
pub mod mat3;
pub mod pid;
pub mod profiles;
pub mod pyjson;
pub mod pyrand;
pub mod rails;
pub mod reason;
pub mod replay_wire;
pub mod state_store;
pub mod thompson;
pub mod vegas;
