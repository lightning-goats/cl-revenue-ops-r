//! Vegas mempool-spike floor (port of the Vegas reflex in
//! `modules/fee_controller.py:2328-2401`; decay BEFORE spike check is
//! load-bearing — see [`vegas_update`]).
//!
//! Protects against arbitrageurs draining channels during high on-chain
//! fee spikes by dynamically raising fee floors. Defenses ported:
//! - CRITICAL-01: exponential decay prevents a permanent latch (no DoS via
//!   fee spamming to keep intensity pinned).
//! - HIGH-03: probabilistic early trigger at 200-400% spikes.

use crate::pyrand::{DecisionEntropy, DecisionInputError, PyRandom};
use revops_econ::pyfloat::py_pow;

/// `VegasReflexState` (py 2328-2351).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VegasReflexState {
    /// Range 0.0 to 1.0.
    pub intensity: f64,
    /// Per-cycle decay factor (~30min half-life at 0.85, py default).
    pub decay_rate: f64,
    /// Last observed sat/vB rate.
    pub last_sat_vb: f64,
    /// Unix timestamp of last update.
    pub last_update: i64,
    /// Confirmation-window counter for consecutive 2x+ spikes.
    pub consecutive_spikes: i64,
}

impl Default for VegasReflexState {
    /// py dataclass field defaults (2347-2351).
    fn default() -> Self {
        Self {
            intensity: 0.0,
            decay_rate: 0.85,
            last_sat_vb: 1.0,
            last_update: 0,
            consecutive_spikes: 0,
        }
    }
}

/// `VegasReflexState.update` (py 2353-2389) verbatim.
///
/// Invariant (documented in the Python source, py 2368-2369): decay is
/// applied FIRST, before the spike check, so a spike that sets intensity
/// to 1.0 this cycle is not immediately reduced again in the SAME cycle.
///
/// RNG-consumption trap: Python's trigger condition is `consecutive_spikes
/// >= 2 or random.random() < boost * 0.5` — short-circuited `or`, so
/// `random()` is called ONLY when `consecutive_spikes < 2` (i.e. only in
/// the 2x..4x branch, and only when the left operand is false). Consuming
/// a draw unconditionally would desync the RNG stream from Python's;
/// `rng.random()` below must stay behind `||`'s short-circuit exactly as
/// written (Rust `||` short-circuits identically to Python `or`).
pub fn vegas_update(
    s: &mut VegasReflexState,
    current_sat_vb: f64,
    ma_sat_vb: f64,
    rng: &mut PyRandom,
    now: i64,
) {
    vegas_update_with_entropy(s, current_sat_vb, ma_sat_vb, rng, now)
        .expect("PyRandom with a static non-empty label cannot fail");
}

pub fn vegas_update_with_entropy(
    s: &mut VegasReflexState,
    current_sat_vb: f64,
    ma_sat_vb: f64,
    rng: &mut dyn DecisionEntropy,
    now: i64,
) -> Result<(), DecisionInputError> {
    let ma_sat_vb = if ma_sat_vb <= 0.0 { 1.0 } else { ma_sat_vb };
    let spike_ratio = current_sat_vb / ma_sat_vb;

    // Decay FIRST (before spike check) so a spike setting intensity to 1.0
    // is not immediately reduced in the same cycle.
    s.intensity *= s.decay_rate;

    // Track consecutive spikes for the confirmation window.
    if spike_ratio >= 2.0 {
        s.consecutive_spikes += 1;
    } else {
        s.consecutive_spikes = 0;
    }

    if spike_ratio >= 4.0 {
        // Immediate trigger: max intensity (>400% spike).
        s.intensity = 1.0;
    } else if spike_ratio >= 2.0 {
        // HIGH-03 defense: probabilistic boost for 200-400% spikes.
        let boost = (spike_ratio - 2.0) / 2.0;

        // TRAP: keep this short-circuited exactly like Python's `or` —
        // `rng.random()` must be evaluated ONLY when `consecutive_spikes <
        // 2` (i.e. only when the left side is false).
        if s.consecutive_spikes >= 2 || rng.random("vegas.boost")? < boost * 0.5 {
            s.intensity = (s.intensity + boost * 0.3).min(1.0);
        }
    }
    s.last_sat_vb = current_sat_vb;
    s.last_update = now;
    Ok(())
}

/// `VegasReflexState.get_floor_multiplier` (py 2391-2401) verbatim.
///
/// Uses `py_pow(_, 0.5)`, NOT `.sqrt()` or a bare `.powf(0.5)`: CPython's
/// `self.intensity ** 0.5` calls `pow()`, which disagrees with `sqrt()` in
/// the last bit for ~0.09% of inputs (same landmine already documented in
/// `thompson::serde` for `** 2`) — verified empirically against CPython
/// for this port. `.sqrt()` (and a bare `.powf(0.5)` under `-O2`+, where
/// LLVM rewrites `powf(_, 0.5)` into `sqrt`) both diverge from CPython's
/// `pow` in the same fraction of cases; `.powf(0.5)` under this
/// workspace's unoptimized (`opt-level = 0`) dev/test profile calls the
/// real libm `pow()` and matches CPython exactly, but a `--release` build
/// hits the LLVM rewrite. `revops_econ::pyfloat::py_pow` (T6 review
/// adjudication: the libm crate reproduces the WRONG sqrt-identical bits,
/// and extern-C FFI is still rewritten for powf(2.0) besides violating
/// forbid(unsafe_code)) black_box-guards both operands, which blocks the
/// rewrite and matches CPython in both profiles — see that function's doc
/// comment for the full story and the workspace-wide migration.
pub fn vegas_floor_multiplier(s: &VegasReflexState) -> f64 {
    if s.intensity < 0.01 {
        1.0
    } else {
        1.0 + py_pow(s.intensity, 0.5) * 2.0
    }
}
