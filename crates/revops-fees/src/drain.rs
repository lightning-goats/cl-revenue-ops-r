//! Drain-bias helpers (port of the node-liquidity-aware auto-drain-bias
//! paths in `modules/fee_controller.py`).
//!
//! Node-wide receivable-ratio / drain-pressure helpers for the
//! node-liquidity-aware auto-drain-bias feature (`fee_controller.py` lines
//! 89-192) plus the per-channel `_drain_fee_multiplier` bias (lines
//! 3069-3087). Kept pure and side-effect free, mirroring the Python
//! module's own docstring intent: "later wiring into `_drain_fee_multiplier`
//! stays unit-testable in isolation." Per the design's no-double-count
//! invariant, these must never read any per-peer drain_direction hint —
//! node-aggregate liquidity only.

/// A single channel's admission-relevant liquidity fields, as consumed by
/// [`compute_node_receivable_ratio`]. Mirrors the subset of a
/// `listpeerchannels`-shaped dict that `fee_controller.py` lines 100-110
/// reads (`state`, `to_us_msat`, `total_msat`); Rust's type system already
/// guarantees well-formed entries, so the Python "skip non-dict entries"
/// defensive branch has no analogue here — every element in the slice is
/// checked only for `state`.
#[derive(Debug, Clone)]
pub struct NodeChannel {
    pub state: String,
    pub to_us_msat: i64,
    pub total_msat: i64,
}

/// Port of `fee_controller.compute_node_receivable_ratio` (lines 89-116).
///
/// `receivable_ratio = total_remote / total_capacity = 1 - (total_local /
/// total_capacity)` over active (`CHANNELD_NORMAL`) channels only. A
/// source-heavy node (mostly local balance) has a LOW receivable ratio; a
/// sink-heavy node (mostly remote balance) has a HIGH receivable ratio.
///
/// Returns `1.0` (neutral/no-drain-pressure) when there is no active
/// capacity.
pub fn compute_node_receivable_ratio(channels: &[NodeChannel]) -> f64 {
    let mut total_local: i64 = 0;
    let mut total_capacity: i64 = 0;
    for ch in channels {
        if ch.state != "CHANNELD_NORMAL" {
            continue;
        }
        total_local += ch.to_us_msat;
        total_capacity += ch.total_msat;
    }

    if total_capacity == 0 {
        return 1.0;
    }

    ((total_capacity - total_local) as f64) / (total_capacity as f64)
}

/// Port of `fee_controller.node_drain_pressure` (lines 118-137). Linear
/// ramp of node-level drain pressure in `[0.0, 1.0]`.
///
/// `0.0` when `receivable_ratio >= target` (node healthy/balanced — no
/// drain pressure). `1.0` when `receivable_ratio <= floor` (node
/// starved/source-heavy — full drain pressure). Linear in between:
/// `(target - receivable_ratio) / (target - floor)`, clamped to `[0, 1]`.
///
/// Degenerate guard: if `target <= floor` (misconfiguration), avoid
/// div-by-zero by returning `1.0` when at/below floor, else `0.0`.
pub fn node_drain_pressure(receivable_ratio: f64, target: f64, floor: f64) -> f64 {
    if target <= floor {
        return if receivable_ratio <= floor { 1.0 } else { 0.0 };
    }

    if receivable_ratio >= target {
        return 0.0;
    }
    if receivable_ratio <= floor {
        return 1.0;
    }

    let pressure = (target - receivable_ratio) / (target - floor);
    pressure.clamp(0.0, 1.0)
}

/// Port of `fee_controller.effective_drain_discount_max` (lines 167-185).
///
/// Extends the static, operator-set `drain_fee_discount_max` with a
/// node-aggregate starvation term: `bias_max * node_pressure`. The
/// effective cap is the LARGER of the two, so a static discount the
/// operator already configured is never reduced by this feature, and a
/// source-heavy (starved) node auto-activates a discount even when the
/// static cap is `0.0`.
///
/// When `bias_enabled` is `false`, returns `static_max` unchanged
/// (byte-identical to the pre-node-liquidity-bias behavior) regardless of
/// `node_pressure` — this is the default-off invariant. Result is always
/// `>= static_max`.
///
/// Python's `effective_drain_discount_max(cfg_like, node_pressure)` reads
/// `static_max`/`bias_enabled`/`bias_max` off a loosely-typed cfg object via
/// `_cfg_float`/`_cfg_bool`, which defensively guard against mocked
/// attributes auto-vivifying truthy non-bool/non-numeric values (see the
/// Python docstrings on those helpers). Rust's static typing already
/// enforces `bias_enabled: bool` and `static_max`/`bias_max: f64` at the
/// call boundary, so that defensive coercion has no work left to do here —
/// the signature takes the already-resolved values directly.
pub fn effective_drain_discount_max(
    static_max: f64,
    bias_enabled: bool,
    bias_max: f64,
    node_pressure: f64,
) -> f64 {
    if !bias_enabled {
        return static_max;
    }
    static_max.max(bias_max * node_pressure)
}

/// Port of `FeeController._drain_fee_multiplier` (lines 3069-3087). Bounded
/// discount for stagnant over-local channels.
///
/// Returns `1.0` (no-op) unless the channel is above the high-liquidity
/// threshold, had zero forwards in the observation window, and the
/// operator enabled a non-zero `discount_max`. Discount scales linearly
/// with the excess above the threshold and is clamped to `discount_max`.
/// Rails (`min_fee_ppm`) still apply downstream — this is a bias, not an
/// override.
pub fn drain_fee_multiplier(
    local_ratio: f64,
    forward_count: i64,
    high_liquidity_threshold: f64,
    discount_max: f64,
) -> f64 {
    if discount_max <= 0.0 || forward_count > 0 {
        return 1.0;
    }
    if local_ratio <= high_liquidity_threshold || high_liquidity_threshold >= 1.0 {
        return 1.0;
    }
    let excess = (local_ratio - high_liquidity_threshold) / (1.0 - high_liquidity_threshold);
    1.0 - discount_max.min(discount_max * excess)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receivable_ratio_no_capacity_is_neutral() {
        assert_eq!(compute_node_receivable_ratio(&[]), 1.0);
    }

    #[test]
    fn receivable_ratio_skips_non_normal_state() {
        let chans = [
            NodeChannel {
                state: "CHANNELD_NORMAL".into(),
                to_us_msat: 200_000_000,
                total_msat: 1_000_000_000,
            },
            NodeChannel {
                state: "CHANNELD_AWAITING_LOCKIN".into(),
                to_us_msat: 999_000_000,
                total_msat: 1_000_000_000,
            },
        ];
        assert_eq!(compute_node_receivable_ratio(&chans), 0.8);
    }

    #[test]
    fn drain_pressure_degenerate_guard() {
        assert_eq!(node_drain_pressure(0.6, 0.3, 0.3), 0.0);
        assert_eq!(node_drain_pressure(0.3, 0.3, 0.3), 1.0);
    }

    #[test]
    fn default_off_invariant_ignores_pressure() {
        for pressure in [0.0, 0.25, 0.5, 0.75, 1.0] {
            assert_eq!(effective_drain_discount_max(0.2, false, 0.9, pressure), 0.2);
        }
    }

    #[test]
    fn drain_fee_multiplier_no_op_when_forwards_present() {
        assert_eq!(drain_fee_multiplier(0.9, 3, 0.8, 0.3), 1.0);
    }
}
