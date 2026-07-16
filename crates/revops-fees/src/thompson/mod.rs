//! Discounted Gaussian Thompson sampling (port of
//! `GaussianThompsonState` in `modules/fee_controller.py`): Bayesian
//! quadratic regression over the 3x3 kernel in `crate::mat3`, with
//! injected `crate::pyrand::PyRandom` draws.
//!
//! Submodules are filled in by Phase 4 Tasks 2 (recompute), 7 (dynamics +
//! sampling), and 3/9 (serde).

pub mod dynamics;
pub mod recompute;
pub mod sampling;
pub mod serde;
