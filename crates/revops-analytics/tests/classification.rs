//! Phase 3 Task 2 (Wave 1): classification authority tests.
//!
//! Two families:
//! - The 5 vendored `role30d_*` golden fixtures (`fixtures/golden/
//!   profitability/`), replayed exactly. Fixtures store Python enum NAMEs
//!   (uppercase) for `lifetime_role` / `role_30d`.
//! - The hysteresis/veto/threshold table transcribed from
//!   `cl_revenue_ops-port/tests/test_classification_authority.py`
//!   (`TestFlowStateDecision`, `TestRevenueRoleDecision`).
//!
//! A drift-guard test also asserts the vendored fixture bytes match the
//! port-worktree source bytes when that worktree is checked out locally
//! (same pattern as conformance's `corpus_is_byte_identical_to_source`).

use std::path::{Path, PathBuf};

use revops_analytics::classification::{
    classify_balance_position, flow_state, revenue_role_30d, ChannelRole, ChannelState,
    BALANCED_ACTIVE_TURNOVER_THRESHOLD, DORMANT_KALMAN_RATIO_THRESHOLD, KALMAN_BALANCE_VETO_RATIO,
    ROLE_DIRECTIONAL_RATIO, ROLE_MIN_FORWARDS_30D, SINK_ENTER_OUTBOUND_RATIO,
    SINK_EXIT_OUTBOUND_RATIO, SOURCE_ENTER_OUTBOUND_RATIO, SOURCE_EXIT_OUTBOUND_RATIO,
};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Enum wire duality (as_value / as_name)
// ---------------------------------------------------------------------------

#[test]
fn channel_state_wire_values_and_names() {
    let cases: &[(ChannelState, &str, &str)] = &[
        (ChannelState::Source, "source", "SOURCE"),
        (ChannelState::Sink, "sink", "SINK"),
        (ChannelState::Balanced, "balanced", "BALANCED"),
        (
            ChannelState::BalancedActive,
            "balanced_active",
            "BALANCED_ACTIVE",
        ),
        (ChannelState::Dormant, "dormant", "DORMANT"),
        (ChannelState::Unknown, "unknown", "UNKNOWN"),
        (ChannelState::Congested, "congested", "CONGESTED"),
    ];
    for (state, value, name) in cases {
        assert_eq!(state.as_value(), *value);
        assert_eq!(state.as_name(), *name);
    }
}

#[test]
fn channel_state_is_balanced() {
    assert!(ChannelState::Balanced.is_balanced());
    assert!(ChannelState::BalancedActive.is_balanced());
    for state in [
        ChannelState::Source,
        ChannelState::Sink,
        ChannelState::Dormant,
        ChannelState::Unknown,
        ChannelState::Congested,
    ] {
        assert!(!state.is_balanced());
    }
}

#[test]
fn channel_role_wire_values_and_names() {
    let cases: &[(ChannelRole, &str, &str)] = &[
        (
            ChannelRole::InboundGateway,
            "inbound_gateway",
            "INBOUND_GATEWAY",
        ),
        (
            ChannelRole::OutboundGateway,
            "outbound_gateway",
            "OUTBOUND_GATEWAY",
        ),
        (ChannelRole::Balanced, "balanced", "BALANCED"),
        (ChannelRole::Dormant, "dormant", "DORMANT"),
    ];
    for (role, value, name) in cases {
        assert_eq!(role.as_value(), *value);
        assert_eq!(role.as_name(), *name);
    }
}

// ---------------------------------------------------------------------------
// Constants verbatim (classification.py lines 82-92)
// ---------------------------------------------------------------------------

#[test]
fn constants_match_python_verbatim() {
    assert_eq!(BALANCED_ACTIVE_TURNOVER_THRESHOLD, 0.01);
    assert_eq!(DORMANT_KALMAN_RATIO_THRESHOLD, 0.01);
    assert_eq!(SINK_ENTER_OUTBOUND_RATIO, 0.78);
    assert_eq!(SINK_EXIT_OUTBOUND_RATIO, 0.72);
    assert_eq!(SOURCE_ENTER_OUTBOUND_RATIO, 0.22);
    assert_eq!(SOURCE_EXIT_OUTBOUND_RATIO, 0.28);
    assert_eq!(KALMAN_BALANCE_VETO_RATIO, 0.05);
    assert_eq!(ROLE_MIN_FORWARDS_30D, 10);
    assert_eq!(ROLE_DIRECTIONAL_RATIO, 0.70);
}

// ---------------------------------------------------------------------------
// TestFlowStateDecision (transcribed from test_classification_authority.py)
// ---------------------------------------------------------------------------

#[test]
fn flow_state_threshold_stage() {
    // common = source_threshold=0.05, sink_threshold=-0.05, outbound_ratio=0.5,
    // previous_state=None, turnover=0.02
    let call = |kalman_ratio: f64| flow_state(kalman_ratio, 0.05, -0.05, 0.5, None, 0.02);
    assert_eq!(call(0.10), ChannelState::Source);
    assert_eq!(call(-0.10), ChannelState::Sink);
    assert_eq!(call(0.0), ChannelState::BalancedActive);
}

#[test]
fn hysteresis_bands() {
    // A prior SINK stays SINK down to the exit band (0.72 < r < 0.78).
    assert_eq!(
        classify_balance_position(0.75, Some("sink"), 0.0, 0.0),
        ChannelState::Sink
    );
    assert_ne!(
        classify_balance_position(0.75, None, 0.0, 0.0),
        ChannelState::Sink
    );
}

#[test]
fn hysteresis_bands_full_table() {
    // Fresh channel (no previous state): enters SINK only strictly above the
    // enter band (0.78), enters SOURCE only strictly below the enter band
    // (0.22).
    assert_eq!(
        classify_balance_position(0.78, None, 0.0, 0.0),
        ChannelState::Dormant,
        "exactly at the enter band is NOT sink (strict >)"
    );
    assert_eq!(
        classify_balance_position(0.781, None, 0.0, 0.0),
        ChannelState::Sink
    );
    assert_eq!(
        classify_balance_position(0.22, None, 0.0, 0.0),
        ChannelState::Dormant,
        "exactly at the enter band is NOT source (strict <)"
    );
    assert_eq!(
        classify_balance_position(0.219, None, 0.0, 0.0),
        ChannelState::Source
    );

    // Previous SINK: stays SINK until outbound_ratio drops to/below the exit
    // band (0.72); previous SOURCE has the mirrored asymmetry at 0.28.
    assert_eq!(
        classify_balance_position(0.73, Some("sink"), 0.0, 0.0),
        ChannelState::Sink
    );
    assert_eq!(
        classify_balance_position(0.72, Some("sink"), 0.0, 0.0),
        ChannelState::Dormant,
        "exactly at the exit band is NOT sink (strict >)"
    );
    assert_eq!(
        classify_balance_position(0.27, Some("source"), 0.0, 0.0),
        ChannelState::Source
    );
    assert_eq!(
        classify_balance_position(0.28, Some("source"), 0.0, 0.0),
        ChannelState::Dormant,
        "exactly at the exit band is NOT source (strict <)"
    );

    // Previous state comparison is case-insensitive (Python `.lower()`).
    assert_eq!(
        classify_balance_position(0.73, Some("SINK"), 0.0, 0.0),
        ChannelState::Sink
    );
    assert_eq!(
        classify_balance_position(0.73, Some("Sink"), 0.0, 0.0),
        ChannelState::Sink
    );
}

#[test]
fn direction_veto() {
    // Draining (positive kalman beyond veto) forbids SINK label.
    assert_ne!(
        classify_balance_position(0.90, None, 0.10, 0.0),
        ChannelState::Sink
    );
    // Filling forbids SOURCE label.
    assert_ne!(
        classify_balance_position(0.10, None, -0.10, 0.0),
        ChannelState::Source
    );
}

#[test]
fn direction_veto_boundary_is_strict() {
    // kalman_ratio exactly at the veto threshold (0.05) does NOT veto
    // (Python `>`, not `>=`) — 0.9 outbound with kalman == 0.05 still SINK.
    assert_eq!(
        classify_balance_position(0.90, None, 0.05, 0.0),
        ChannelState::Sink
    );
    assert_eq!(
        classify_balance_position(0.10, None, -0.05, 0.0),
        ChannelState::Source
    );
}

#[test]
fn dormant_vs_balanced() {
    assert_eq!(
        classify_balance_position(0.5, None, 0.0, 0.0),
        ChannelState::Dormant
    );
    assert_eq!(
        classify_balance_position(0.5, None, 0.03, 0.0),
        ChannelState::Balanced
    );
}

#[test]
fn dormant_kalman_boundary_is_strict() {
    // abs(kalman_ratio) < 0.01 is DORMANT; exactly 0.01 is NOT (falls to
    // BALANCED since turnover is also at/under its own threshold here).
    assert_eq!(
        classify_balance_position(0.5, None, 0.01, 0.0),
        ChannelState::Balanced
    );
    assert_eq!(
        classify_balance_position(0.5, None, -0.01, 0.0),
        ChannelState::Balanced
    );
    assert_eq!(
        classify_balance_position(0.5, None, 0.0099, 0.0),
        ChannelState::Dormant
    );
}

#[test]
fn turnover_boundary_is_strict() {
    // turnover > 0.01 -> BALANCED_ACTIVE; turnover == 0.01 exactly is NOT
    // (strict >), so it falls through to the dormant/balanced check.
    assert_eq!(
        classify_balance_position(0.5, None, 0.0, 0.0100001),
        ChannelState::BalancedActive
    );
    assert_eq!(
        classify_balance_position(0.5, None, 0.0, 0.01),
        ChannelState::Dormant,
        "turnover exactly at threshold is NOT balanced_active"
    );
}

#[test]
fn flow_state_threshold_strictness_falls_through_to_balance_position() {
    // kalman_ratio exactly equal to source_threshold/sink_threshold does NOT
    // trigger SOURCE/SINK (Python `>` / `<`, not `>=`/`<=`) — falls through
    // to classify_balance_position.
    let result = flow_state(0.05, 0.05, -0.05, 0.5, None, 0.0);
    assert_ne!(result, ChannelState::Source);
    let result = flow_state(-0.05, 0.05, -0.05, 0.5, None, 0.0);
    assert_ne!(result, ChannelState::Sink);
}

// ---------------------------------------------------------------------------
// TestRevenueRoleDecision (transcribed from test_classification_authority.py)
// ---------------------------------------------------------------------------

#[test]
fn revenue_role_30d_matches_golden_scenarios_inline() {
    assert_eq!(
        revenue_role_30d(false, 0, 0, ChannelRole::OutboundGateway),
        ChannelRole::OutboundGateway
    );
    assert_eq!(
        revenue_role_30d(true, 2, 40, ChannelRole::Balanced),
        ChannelRole::InboundGateway
    );
    assert_eq!(
        revenue_role_30d(true, 3, 4, ChannelRole::Balanced),
        ChannelRole::Dormant
    );
    assert_eq!(
        revenue_role_30d(true, 20, 22, ChannelRole::Balanced),
        ChannelRole::Balanced
    );
}

#[test]
fn revenue_role_30d_min_forwards_boundary_is_strict() {
    // total == ROLE_MIN_FORWARDS_30D (10) is NOT dormant (Python `<`, not
    // `<=`); total == 9 is dormant.
    assert_eq!(
        revenue_role_30d(true, 5, 5, ChannelRole::Dormant),
        ChannelRole::Balanced
    );
    assert_eq!(
        revenue_role_30d(true, 4, 5, ChannelRole::Dormant),
        ChannelRole::Dormant
    );
}

#[test]
fn revenue_role_30d_directional_ratio_boundary_is_strict() {
    // inbound_ratio/outbound_ratio exactly at 0.70 is NOT gateway (strict >).
    // 7 sourced / 10 total = 0.70 exactly.
    assert_eq!(
        revenue_role_30d(true, 3, 7, ChannelRole::Dormant),
        ChannelRole::Balanced
    );
    // 8 sourced / 11 total > 0.70 -> INBOUND_GATEWAY.
    assert_eq!(
        revenue_role_30d(true, 3, 8, ChannelRole::Dormant),
        ChannelRole::InboundGateway
    );
}

// ---------------------------------------------------------------------------
// Golden fixtures: fixtures/golden/profitability/role30d_*.json
// ---------------------------------------------------------------------------

fn fixtures_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/golden/profitability")
}

fn role_from_name(name: &str) -> ChannelRole {
    match name {
        "INBOUND_GATEWAY" => ChannelRole::InboundGateway,
        "OUTBOUND_GATEWAY" => ChannelRole::OutboundGateway,
        "BALANCED" => ChannelRole::Balanced,
        "DORMANT" => ChannelRole::Dormant,
        other => panic!("unknown ChannelRole name in fixture: {other}"),
    }
}

fn read_json(path: &Path) -> Value {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

const ROLE30D_FIXTURES: &[&str] = &[
    "role30d_balanced_30d",
    "role30d_exit_30d_dominant_direct",
    "role30d_gateway_30d_dominant_sourced",
    "role30d_no_window_falls_back_to_lifetime",
    "role30d_too_few_forwards_30d_dormant",
];

#[test]
fn role30d_golden_fixtures_replayed_exactly() {
    for name in ROLE30D_FIXTURES {
        let path = fixtures_root().join(format!("{name}.json"));
        let doc = read_json(&path);
        let inputs = &doc["inputs"];

        let window_30d_available = inputs["window_30d_available"]
            .as_bool()
            .unwrap_or_else(|| panic!("{name}: window_30d_available not a bool"));
        let forward_count_30d = inputs["forward_count_30d"]
            .as_i64()
            .unwrap_or_else(|| panic!("{name}: forward_count_30d not an i64"));
        let sourced_forward_count_30d = inputs["sourced_forward_count_30d"]
            .as_i64()
            .unwrap_or_else(|| panic!("{name}: sourced_forward_count_30d not an i64"));
        let lifetime_role_name = inputs["lifetime_role"]
            .as_str()
            .unwrap_or_else(|| panic!("{name}: lifetime_role not a string"));
        let lifetime_role = role_from_name(lifetime_role_name);

        let expected_name = doc["role_30d"]
            .as_str()
            .unwrap_or_else(|| panic!("{name}: role_30d not a string"));
        let expected = role_from_name(expected_name);

        let actual = revenue_role_30d(
            window_30d_available,
            forward_count_30d,
            sourced_forward_count_30d,
            lifetime_role,
        );
        assert_eq!(actual, expected, "fixture {name}: mismatch");
        // Also pin the wire-name round trip (fixture stores enum NAMEs).
        assert_eq!(
            actual.as_name(),
            expected_name,
            "fixture {name}: as_name drift"
        );
    }
}

// ---------------------------------------------------------------------------
// Drift guard: vendored fixture bytes == port-worktree source bytes
// (same pattern as conformance's corpus_is_byte_identical_to_source).
// ---------------------------------------------------------------------------

fn resolve_source_dir() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("REVOPS_PY_ROLE30D_FIXTURES") {
        let pb = PathBuf::from(p);
        return pb.is_dir().then_some(pb);
    }
    let home = std::env::var("HOME").ok()?;
    let default =
        PathBuf::from(home).join("bin/cl_revenue_ops-port/tests/golden/fixtures/profitability");
    default.is_dir().then_some(default)
}

#[test]
fn role30d_fixtures_are_byte_identical_to_source() {
    let Some(source) = resolve_source_dir() else {
        eprintln!("skip role30d_fixtures_are_byte_identical_to_source: source not found");
        return;
    };
    for name in ROLE30D_FIXTURES {
        let filename = format!("{name}.json");
        let a = std::fs::read(source.join(&filename))
            .unwrap_or_else(|e| panic!("read source {filename}: {e}"));
        let b = std::fs::read(fixtures_root().join(&filename))
            .unwrap_or_else(|e| panic!("read vendored {filename}: {e}"));
        assert_eq!(a, b, "byte drift in {filename}");
    }
}
