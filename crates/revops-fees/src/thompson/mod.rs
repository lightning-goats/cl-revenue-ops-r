//! Discounted Gaussian Thompson sampling (port of
//! `GaussianThompsonState` in `modules/fee_controller.py`): Bayesian
//! quadratic regression over the 3x3 kernel in `crate::mat3`, with
//! injected `crate::pyrand::PyRandom` draws.
//!
//! Submodules are filled in by Phase 4 Tasks 2 (recompute), 7 (dynamics +
//! sampling), and 3/9 (serde).
//!
//! This module (Task 3, Wave 1) owns the struct + field-level constants +
//! re-exports. `fee_controller.py:384-462` (dataclass field defaults) and
//! `1721-1940` (`to_dict`/`from_dict`) are the source of truth; the actual
//! (de)serialization logic lives in [`serde`].

pub mod dynamics;
pub mod recompute;
pub mod sampling;
pub mod serde;

use crate::mat3::{M3, V3};
use crate::pyjson::OValue;

// ---------------------------------------------------------------------------
// Class-level constants needed by serde's from_dict (py 258-276). The full
// constant family (SUPPORTED_CEILING_*, ZERO_*, CTX_*, ...) is owned by
// Task 2/7 (recompute/dynamics/sampling); only the ones `serde.rs` touches
// are transcribed here.
// ---------------------------------------------------------------------------

/// `MIN_STD` (py 261): never let uncertainty go below 10 ppm. Used by
/// `from_dict`'s legacy 3-tuple contextual-posterior conversion
/// (`legacy_precision = 1 / max(std^2, MIN_STD^2)`).
pub const MIN_STD: f64 = 10.0;

/// `WEIGHT_SCHEME` (py 275): the current observation-weighting scheme
/// marker. A persisted blob without this exact value (or predating the
/// key entirely) carries legacy outcome-scaled weights that `from_dict`
/// rescales on load.
pub const WEIGHT_SCHEME: &str = "exposure_v2";

/// `ZERO_REVENUE_WEIGHT_FACTOR` (py 276): LEGACY (migration only) — the old
/// zero-revenue-window weight factor, used only to invert legacy weights on
/// load.
pub const ZERO_REVENUE_WEIGHT_FACTOR: f64 = 0.15;

/// `MAX_BIAS_NUDGES` (py 358): security bound on out-of-band nudge memory —
/// `from_dict` keeps only the last `MAX_BIAS_NUDGES` entries of
/// `posterior_bias`.
pub const MAX_BIAS_NUDGES: usize = 50;

/// `EXPLORATION_BOOST_MIN`/`MAX` (py 375-376): clamp bounds for the retired
/// (blob-compat-only) `exploration_boost` field.
pub const EXPLORATION_BOOST_MIN: f64 = 0.75;
pub const EXPLORATION_BOOST_MAX: f64 = 2.0;

/// `CONGESTION_OBS_FLAG` (py 326): 6th observation-tuple element marking a
/// congested-window observation.
pub const CONGESTION_OBS_FLAG: &str = "congestion";

/// `ZERO_PROBE_FLAG` (py 347): 6th observation-tuple element marking an
/// injected zero-revenue probe.
pub const ZERO_PROBE_FLAG: &str = "zero_probe";

/// One observation window: `(fee_ppm, revenue_rate, weight, timestamp,
/// time_bucket)` as a plain 5-tuple, or `(..., flag)` as a 6-tuple where
/// `flag` is [`CONGESTION_OBS_FLAG`] or [`ZERO_PROBE_FLAG`]
/// (`fee_controller.py:388-389`, `799-825`). A legacy persisted 4-tuple
/// `(fee, revenue_rate, weight, ts)` (no `time_bucket`) is normalized to a
/// 5-tuple with `time_bucket = "normal"` on load (py 1774-1776).
///
/// `extra` preserves any elements beyond the 6th verbatim — Python tuples
/// are never longer than 6 today, but a future schema version might add a
/// 7th, and this port must not silently drop it (lossless-round-trip is
/// the load-bearing contract for this whole module).
///
/// ## `fee`/`fee_is_int`
///
/// Like `GaussianThompsonState::prior_mean_fee`/`prior_mean_fee_is_int`
/// (see that struct's doc comment), Python's `from_dict`/`to_dict` never
/// casts an observation tuple's fee element (`t[0]`) — a persisted blob's
/// number TEXT survives untouched. Real production `v2_state_json` blobs
/// from the `thompson_aimd_v1` era may carry a JSON float fee
/// (`[250.0, ...]`); re-emitting it as `250` (an int) would be a
/// byte-mismatch against Python's own round trip. `fee_is_int` records
/// whether the source JSON had no decimal point, so `to_dict` can re-emit
/// the same representation. `fee` itself stays the single numeric
/// accessor other code (T2's recompute functions) reads.
#[derive(Debug, Clone, PartialEq)]
pub struct Observation {
    pub fee: f64,
    /// `true` when the persisted fee had no JSON decimal point (a Python
    /// `int`), so `to_dict` re-emits it as a bare integer rather than
    /// `N.0`. See the struct doc comment.
    pub fee_is_int: bool,
    pub revenue_rate: f64,
    pub weight: f64,
    pub ts: i64,
    pub time_bucket: String,
    pub flag: Option<String>,
    pub extra: Vec<OValue>,
}

/// A contextual posterior: `(mean, precision, count, last_update)` — the
/// current 4-tuple layout (`fee_controller.py:412-415`). A legacy
/// persisted 3-tuple `(mean, std, count)` is converted on load exactly as
/// `from_dict` does: `precision = 1 / max(std^2, MIN_STD^2)`,
/// `last_update = 0` (py 1796-1804). `was_legacy_3tuple` records which
/// shape the value arrived in — both `sample_fee_contextual` and
/// `update_contextual` (Task 7) must ALSO accept a raw 3-tuple at runtime
/// (py re-checks `len(obs)` at every read site, not just on load), so this
/// flag lets those call sites recover the original shape without
/// re-parsing.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CtxPosterior {
    pub mean: f64,
    pub precision: f64,
    pub count: i64,
    pub last_update: i64,
    pub was_legacy_3tuple: bool,
}

/// Gaussian Thompson Sampling state for continuous fee optimization
/// (`GaussianThompsonState`, `fee_controller.py:241-462`). Field types and
/// names mirror the Phase 4 plan's Task 3 interface exactly (frozen —
/// Tasks 2/7 depend on this shape).
///
/// ## The `prior_mean_fee`/`prior_std_fee` number-passthrough contract
///
/// These are declared `int` in the Python dataclass (defaults 200/100) but
/// every arithmetic use (`_recompute_posterior_core`, `sample_fee`, ...)
/// treats them as floats, and neither `to_dict` nor `from_dict` casts them
/// (`state.prior_mean_fee = d.get("prior_mean_fee", 200)` — no `float()`).
/// So a persisted blob's number TEXT (`200` vs `200.0`) survives untouched
/// through a from_dict/to_dict cycle that never recomputes them. Storing
/// only `f64` here would lose that distinction (`200.0_f64` doesn't remember
/// whether it came from JSON `200` or `200.0`), so `serde.rs` also tracks
/// `prior_mean_fee_is_int`/`prior_std_fee_is_int` (not part of the frozen
/// interface list, but required for byte-identical round-trips — analogous
/// to `CtxPosterior::was_legacy_3tuple` above).
#[derive(Debug, Clone, PartialEq)]
pub struct GaussianThompsonState {
    pub prior_mean_fee: f64,
    pub prior_std_fee: f64,
    /// See the struct doc: `true` when the persisted value had no JSON
    /// decimal point (i.e. was a Python `int`), so `to_dict` re-emits it as
    /// a bare integer rather than `N.0`.
    pub prior_mean_fee_is_int: bool,
    pub prior_std_fee_is_int: bool,

    pub observations: Vec<Observation>,

    pub posterior_mean: f64,
    pub posterior_std: f64,
    pub posterior_coeffs: V3,
    pub posterior_precision: M3,
    pub noise_variance: f64,

    pub prior_coeffs: V3,
    pub prior_precision: M3,

    pub last_fee_min: f64,
    pub last_fee_max: f64,

    /// Ordered map (Python dict, insertion-order preserved) —
    /// `contextual_posteriors`, keyed by context key
    /// (`"{balance}:{time_bucket}:{role}"`).
    pub contextual_posteriors: Vec<(String, CtxPosterior)>,

    pub posterior_bias: Vec<(f64, f64, i64)>,

    pub charged_fee_mean: f64,

    pub zero_revenue_streak: i64,
    pub zero_run_start_fee: f64,
    pub zero_run_start_ts: i64,

    pub positive_rate_ref: f64,
    pub positive_rate_ref_ts: i64,

    pub meaningful_gap_ema_hours: f64,
    pub last_meaningful_ts: i64,

    pub last_upward_probe_ts: i64,

    /// Retired one-shot exploration multiplier; blob-compat only.
    pub exploration_boost: f64,

    pub last_sampled_fee: i64,
    pub last_sample_time: i64,

    /// Retired one-shot prior re-seed marker; blob-compat only.
    pub reseeded_at: i64,

    /// Unknown top-level keys from the source dict, preserved in the order
    /// first encountered, and re-emitted (by `serde::gts_to_dict`) AFTER
    /// all known keys. Python's own `to_dict`/`from_dict` has no such
    /// passthrough (unknown keys are silently dropped) — this port is
    /// intentionally MORE lossless than Python, which matters for a mixed
    /// Python/Rust rollout where either side might persist a field the
    /// other doesn't know about yet.
    pub extra: Vec<(String, OValue)>,
}

impl Default for GaussianThompsonState {
    fn default() -> Self {
        GaussianThompsonState {
            prior_mean_fee: 200.0,
            prior_std_fee: 100.0,
            prior_mean_fee_is_int: true,
            prior_std_fee_is_int: true,
            observations: Vec::new(),
            posterior_mean: 200.0,
            posterior_std: 100.0,
            posterior_coeffs: [0.0, 1.0, 0.0],
            posterior_precision: [[0.01, 0.0, 0.0], [0.0, 0.01, 0.0], [0.0, 0.0, 0.01]],
            noise_variance: 1000.0,
            prior_coeffs: [0.0, 1.0, 0.0],
            prior_precision: [[0.01, 0.0, 0.0], [0.0, 0.01, 0.0], [0.0, 0.0, 0.01]],
            last_fee_min: 0.0,
            last_fee_max: 0.0,
            contextual_posteriors: Vec::new(),
            posterior_bias: Vec::new(),
            charged_fee_mean: 0.0,
            zero_revenue_streak: 0,
            zero_run_start_fee: 0.0,
            zero_run_start_ts: 0,
            positive_rate_ref: 0.0,
            positive_rate_ref_ts: 0,
            meaningful_gap_ema_hours: 0.0,
            last_meaningful_ts: 0,
            last_upward_probe_ts: 0,
            exploration_boost: 1.0,
            last_sampled_fee: 0,
            last_sample_time: 0,
            reseeded_at: 0,
            extra: Vec::new(),
        }
    }
}
