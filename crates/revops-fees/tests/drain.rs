//! Drain-bias helper parity (`modules/fee_controller.py` lines 89-192,
//! 3069-3087: `compute_node_receivable_ratio`, `node_drain_pressure`,
//! `effective_drain_discount_max`, `_drain_fee_multiplier`).
//!
//! No generator subcommand exists for `drain` yet (unlike `pyrand`/`mat3`
//! from Task 1) — adding one would mean editing
//! `~/bin/cl_revenue_ops-port/tools/port/gen_fees_fixtures.py`, a file the
//! Wave-1 Task 5 agent is concurrently extending with its own `rails`
//! subcommand in the SAME sibling repo, outside this task's worktree
//! isolation. Per this task's file-scope contract (crates/revops-fees +
//! tests + vendored goldens ONLY, touch nothing shared), the pinned vectors
//! below were instead produced by directly invoking the REAL Python
//! functions (`modules.fee_controller.compute_node_receivable_ratio`,
//! `node_drain_pressure`, `effective_drain_discount_max`,
//! `FeeController._drain_fee_multiplier` — the oracle, not a
//! reimplementation) from a throwaway script against the unmodified
//! checkout, and transcribed here as `repr(float)` strings — same wire
//! format and same `py_repr(actual) == expected` comparison discipline as
//! every other fixture in this crate. A `drain` generator subcommand should
//! be added to `gen_fees_fixtures.py` by whichever task next owns that file
//! (see phase4-task4-report.md).

use revops_econ::pyfloat::py_repr;
use revops_fees::drain::{
    compute_node_receivable_ratio, drain_fee_multiplier, effective_drain_discount_max,
    node_drain_pressure, NodeChannel,
};

fn assert_repr(actual: f64, expected_repr: &str) {
    assert_eq!(py_repr(actual), expected_repr);
}

fn normal(to_us_msat: i64, total_msat: i64) -> NodeChannel {
    NodeChannel {
        state: "CHANNELD_NORMAL".to_string(),
        to_us_msat,
        total_msat,
    }
}

fn other_state(to_us_msat: i64, total_msat: i64) -> NodeChannel {
    NodeChannel {
        state: "CHANNELD_AWAITING_LOCKIN".to_string(),
        to_us_msat,
        total_msat,
    }
}

// ---------------------------------------------------------------------
// compute_node_receivable_ratio — 10 pinned vectors (py 89-116).
// ---------------------------------------------------------------------

#[test]
fn receivable_ratio_pinned_vectors() {
    // (name, channels, expected repr) — values transcribed from a direct
    // call to the real `modules.fee_controller.compute_node_receivable_ratio`.
    assert_repr(compute_node_receivable_ratio(&[]), "1.0"); // empty
    assert_repr(compute_node_receivable_ratio(&[normal(0, 0)]), "1.0"); // no_capacity_all_zero_total
    assert_repr(
        compute_node_receivable_ratio(&[normal(500_000_000, 1_000_000_000)]),
        "0.5",
    ); // single_balanced
    assert_repr(
        compute_node_receivable_ratio(&[normal(900_000_000, 1_000_000_000)]),
        "0.1",
    ); // single_source_heavy
    assert_repr(
        compute_node_receivable_ratio(&[normal(100_000_000, 1_000_000_000)]),
        "0.9",
    ); // single_sink_heavy
    assert_repr(
        compute_node_receivable_ratio(&[
            normal(200_000_000, 1_000_000_000),
            other_state(999_000_000, 1_000_000_000),
        ]),
        "0.8",
    ); // mixed_states_skip_non_normal
    assert_repr(
        compute_node_receivable_ratio(&[normal(1_000_000_000, 1_000_000_000)]),
        "0.0",
    ); // all_local
    assert_repr(
        compute_node_receivable_ratio(&[normal(0, 1_000_000_000)]),
        "1.0",
    ); // all_remote
    assert_repr(
        compute_node_receivable_ratio(&[
            normal(300_000_000, 1_000_000_000),
            normal(100_000_000, 2_000_000_000),
        ]),
        "0.8666666666666667",
    ); // multi_channel_aggregate
    assert_repr(
        compute_node_receivable_ratio(&[
            normal(250_000_000, 1_000_000_000),
            normal(150_000_000, 500_000_000),
            other_state(999_000_000, 1_000_000_000),
        ]),
        "0.7333333333333333",
    ); // three_channel_mixed_states
}

// ---------------------------------------------------------------------
// node_drain_pressure — 10 pinned vectors incl. the degenerate
// target<=floor guard (py 118-137).
// ---------------------------------------------------------------------

#[test]
fn node_drain_pressure_pinned_vectors() {
    assert_repr(node_drain_pressure(0.8, 0.5, 0.2), "0.0"); // above_target_zero
    assert_repr(node_drain_pressure(0.5, 0.5, 0.2), "0.0"); // at_target_zero
    assert_repr(node_drain_pressure(0.35, 0.5, 0.2), "0.5000000000000001"); // mid_ramp
    assert_repr(node_drain_pressure(0.2, 0.5, 0.2), "1.0"); // at_floor_one
    assert_repr(node_drain_pressure(0.1, 0.5, 0.2), "1.0"); // below_floor_one
    assert_repr(node_drain_pressure(0.6, 0.3, 0.3), "0.0"); // degenerate_target_eq_floor_above
    assert_repr(node_drain_pressure(0.3, 0.3, 0.3), "1.0"); // degenerate_target_eq_floor_at_floor
    assert_repr(node_drain_pressure(0.6, 0.2, 0.3), "0.0"); // degenerate_target_lt_floor_above
    assert_repr(node_drain_pressure(0.2, 0.2, 0.3), "1.0"); // degenerate_target_lt_floor_at_floor
    assert_repr(node_drain_pressure(0.425, 0.5, 0.2), "0.25000000000000006"); // ramp_quarter
}

/// Default-off invariant: bias disabled must return the static max
/// unchanged for ALL node-pressure values, byte-identical to the
/// pre-node-liquidity-bias behavior (py 178-183 docstring guarantee).
#[test]
fn effective_drain_discount_max_default_off_invariant() {
    for pressure in [0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0] {
        assert_eq!(
            effective_drain_discount_max(0.2, false, 0.9, pressure),
            0.2,
            "pressure={pressure}"
        );
        // Even a static_max of 0.0 stays 0.0 when disabled.
        assert_eq!(effective_drain_discount_max(0.0, false, 0.9, pressure), 0.0);
    }
}

/// Result is always >= static_max (py 172 docstring invariant), and the
/// pinned vectors below trace 6 vectors against the real
/// `effective_drain_discount_max`.
#[test]
fn effective_drain_discount_max_pinned_vectors() {
    assert_repr(effective_drain_discount_max(0.0, false, 0.5, 1.0), "0.0"); // default_off_static_zero
    assert_repr(effective_drain_discount_max(0.2, false, 0.9, 1.0), "0.2"); // default_off_static_nonzero_unaffected
    assert_repr(effective_drain_discount_max(0.3, true, 0.2, 0.5), "0.3"); // enabled_bias_below_static: max(0.3, 0.2*0.5=0.1) = 0.3
    assert_repr(effective_drain_discount_max(0.1, true, 0.5, 1.0), "0.5"); // enabled_bias_above_static: max(0.1, 0.5*1.0=0.5) = 0.5
    assert_repr(effective_drain_discount_max(0.1, true, 0.5, 0.0), "0.1"); // enabled_zero_pressure: max(0.1, 0.5*0.0=0.0) = 0.1
    assert_repr(effective_drain_discount_max(0.05, true, 0.4, 1.0), "0.4"); // enabled_full_pressure: max(0.05, 0.4*1.0=0.4) = 0.4

    // Invariant check across the pinned set.
    for &(static_max, enabled, bias_max, pressure) in &[
        (0.0, false, 0.5, 1.0),
        (0.2, false, 0.9, 1.0),
        (0.3, true, 0.2, 0.5),
        (0.1, true, 0.5, 1.0),
        (0.1, true, 0.5, 0.0),
        (0.05, true, 0.4, 1.0),
    ] {
        assert!(
            effective_drain_discount_max(static_max, enabled, bias_max, pressure) >= static_max
        );
    }
}

// ---------------------------------------------------------------------
// drain_fee_multiplier — 7 pinned vectors (py 3069-3087).
// ---------------------------------------------------------------------

#[test]
fn drain_fee_multiplier_pinned_vectors() {
    assert_repr(drain_fee_multiplier(0.9, 0, 0.8, 0.0), "1.0"); // no_op_zero_discount
    assert_repr(drain_fee_multiplier(0.9, 3, 0.8, 0.3), "1.0"); // no_op_has_forwards
    assert_repr(drain_fee_multiplier(0.7, 0, 0.8, 0.3), "1.0"); // no_op_below_threshold
    assert_repr(drain_fee_multiplier(0.9, 0, 1.0, 0.3), "1.0"); // no_op_threshold_ge_one
    assert_repr(
        drain_fee_multiplier(0.99, 0, 0.8, 0.3),
        "0.7150000000000001",
    ); // ramp: excess=(0.99-0.8)/0.2=0.95, disc=min(0.3,0.285)=0.285
    assert_repr(drain_fee_multiplier(0.9, 0, 0.8, 0.3), "0.85"); // ramp: excess=0.5, disc=0.15
    assert_repr(drain_fee_multiplier(1.0, 0, 0.5, 0.2), "0.8"); // excess saturates at 1.0 == discount_max
}
