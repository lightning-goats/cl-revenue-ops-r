//! R68-7 (RED): per-producer payload assembly and the publish/withhold
//! decision.
//!
//! R68-6 gave the three producers ownership and a readback-verified
//! publish. What each one actually PUBLISHES is still unported, and the
//! three Python call sites disagree about absence in a way a uniform
//! port gets wrong:
//!
//! - `["revenue", "status"]` (`cl-revenue-ops.py:3706-3717`) is pushed
//!   whenever a data service exists, EVEN IF there is no config: the
//!   `operator_controls.values` sub-dict simply becomes `{}`, and the
//!   same for `fee_decision` when the controller has no
//!   `get_last_decision_summary`.
//! - `["revenue", "fee-bounds"]` (`:3720-3726`) is guarded by
//!   `if cfg_snap and data_service:` -- with no config snapshot it is NOT
//!   pushed at all. Publishing zeros here would be worse than silence:
//!   an external consumer reading `min_fee_ppm: 0` cannot tell a real
//!   floor from a missing config.
//! - `["revenue", "segment-observations"]`
//!   (`modules/rebalance_engine_v2.py:3168-3183`) withholds twice --
//!   `store is None or not observer_member_id`, then
//!   `if not snapshot.get("segment_observations"): return False`.
//!
//! So "do not publish" is a real, distinct outcome that must be
//! DECLARED, never a silent skip and never a publish full of defaults.
//!
//! Scope: the fee-bounds and status payloads, and the withhold decision
//! for all three producers. The segment-observation SNAPSHOT merge needs
//! an `OValue` -> `PyDict` conversion (the frozen
//! `revops_rebalance::segstore` returns `OValue` and deliberately omits
//! `observer_member_id` for the engine to stamp in), so the snapshot
//! body and the owner-cycle wiring are the next slice.

use revops::datastore_producers::{
    fee_bounds_decision, fee_bounds_payload, segment_publish_is_allowed, status_payload,
    PublishDecision, Withheld,
};
use revops_analytics::telemetry::{python_dumps, PyDict, PyVal};

fn dumps(payload: &PyDict) -> String {
    python_dumps(&PyVal::Dict(payload.clone()))
}

// =====================================================================
// fee-bounds: the payload
// =====================================================================

/// py `{"min_fee_ppm": ..., "max_fee_ppm": ..., "mid_fee_ppm": ...}`
/// (`cl-revenue-ops.py:3723-3725`). Key ORDER is part of the contract:
/// `json.dumps` walks a dict in insertion order, and this payload is a
/// dict literal.
#[test]
fn the_fee_bounds_payload_matches_the_python_dict_literal() {
    assert_eq!(
        dumps(&fee_bounds_payload(50, 1000)),
        r#"{"min_fee_ppm": 50, "max_fee_ppm": 1000, "mid_fee_ppm": 525}"#
    );
}

// NOT TESTED HERE, DELIBERATELY: the negative-midpoint divergence.
//
// py computes the midpoint with `//` (floors toward negative infinity)
// while Rust's `/` truncates toward zero, so the two disagree for a
// negative sum -- py `(-3 + 0) // 2 == -2`, Rust `-1`. An earlier draft
// of this file pinned that as a parity contract. It is NOT one, and
// asserting it would have been a mistake: a negative `min_fee_ppm`
// violates the declared policy `CONFIG_FIELD_RANGES['min_fee_ppm'] =
// (5, 100000)`, whose comment reads "CRITICAL-02 FIX: Minimum 5 PPM to
// ensure economic viability" (modules/config.py:355). Encoding the
// arithmetic of an invalid state would make that state look supported.
//
// The state is nevertheless REACHABLE, in both implementations, which is
// a defect filed separately rather than ported: `CONFIG_FIELD_RANGES` is
// consulted at only three sites -- config.py:856 (a hardcoded five-key
// list that does not include `min_fee_ppm`), :1037 (DB override load,
// skip-with-warning) and :1098 (runtime update, error) -- so the CLN
// STARTUP OPTION path (`min_fee_ppm=_safe_int('revenue-ops-min-fee-ppm')`,
// cl-revenue-ops.py:2553) installs its value unchecked. Rust matches on
// the DB path (`db_layer` -> `config_resolve::validate_override`) and
// matches the gap on the option path (`python_layer` returns the raw
// value).
//
// The implementation still uses `div_euclid`, the correct rendering of
// Python's `//` for every input, but no test here claims a negative
// bound is a supported configuration.

#[test]
fn the_midpoint_of_an_odd_span_floors_rather_than_rounding() {
    // py: (5 + 8) // 2 == 6, not 7. Both bounds sit inside the declared
    // policy range, so this pins the arithmetic without asserting
    // anything about configurations policy forbids.
    assert_eq!(
        dumps(&fee_bounds_payload(5, 8)),
        r#"{"min_fee_ppm": 5, "max_fee_ppm": 8, "mid_fee_ppm": 6}"#
    );
}

/// Whatever the arithmetic, the midpoint must summarise the bounds it is
/// published beside. A consumer that clamps to `mid_fee_ppm` would
/// otherwise be pushed outside the operator's own range.
///
/// The pairs are drawn from inside the declared policy range
/// `(5, 100000)` (`modules/config.py:355`), plus its two boundaries --
/// not from the out-of-range states discussed above.
#[test]
fn the_midpoint_lies_within_the_bounds_it_summarises() {
    for (min, max) in [(5, 5), (5, 6), (50, 1000), (5, 100_000), (99_999, 100_000)] {
        let payload = fee_bounds_payload(min, max);
        let mid = payload
            .iter()
            .find(|(k, _)| k == "mid_fee_ppm")
            .map(|(_, v)| v.clone())
            .expect("mid_fee_ppm is present");
        let PyVal::Int(mid) = mid else {
            panic!("mid_fee_ppm must be an int, got {mid:?}");
        };
        assert!(min <= mid && mid <= max, "mid {mid} escaped [{min}, {max}]");
    }
}

// =====================================================================
// fee-bounds: the withhold decision
// =====================================================================

/// py: `cfg_snap = config.snapshot() if config else None` then
/// `if cfg_snap and data_service:`. No snapshot means NO push.
/// Publishing a payload of zeros instead would be indistinguishable, to
/// every external consumer, from an operator who really set the floor to
/// zero.
#[test]
fn an_absent_config_snapshot_withholds_the_fee_bounds_publish_entirely() {
    match fee_bounds_decision(None) {
        PublishDecision::Withhold(reason) => assert_eq!(reason, Withheld::NoConfigSnapshot),
        PublishDecision::Publish(payload) => {
            panic!(
                "absence must not be published as defaults: {}",
                dumps(&payload)
            )
        }
    }
}

#[test]
fn a_present_config_snapshot_publishes_its_bounds() {
    match fee_bounds_decision(Some((50, 1000))) {
        PublishDecision::Publish(payload) => assert_eq!(
            dumps(&payload),
            r#"{"min_fee_ppm": 50, "max_fee_ppm": 1000, "mid_fee_ppm": 525}"#
        ),
        PublishDecision::Withhold(reason) => panic!("a real snapshot publishes: {reason:?}"),
    }
}

// =====================================================================
// status: the payload, and the opposite absence rule
// =====================================================================

/// py `{"operator_controls": {"values": ...}, "fee_decision": ...}`
/// (`cl-revenue-ops.py:3706-3714`).
#[test]
fn the_status_payload_matches_the_python_dict_literal() {
    let mut controls = PyDict::new();
    controls.push("max_fee_ppm", PyVal::Int(1000));
    let mut decision = PyDict::new();
    decision.push("adjusted", PyVal::Int(3));

    assert_eq!(
        dumps(&status_payload(Some(controls), Some(decision))),
        r#"{"operator_controls": {"values": {"max_fee_ppm": 1000}}, "fee_decision": {"adjusted": 3}}"#
    );
}

/// The asymmetry with fee-bounds, and the reason this slice exists.
///
/// py `config.public_runtime_dict() if config else {}` -- status is
/// pushed with an EMPTY values dict rather than withheld. An operator
/// watching this key needs to see that the plugin is alive and holding
/// no config, which silence cannot express.
#[test]
fn status_publishes_an_empty_controls_dict_rather_than_withholding() {
    let mut decision = PyDict::new();
    decision.push("adjusted", PyVal::Int(0));

    assert_eq!(
        dumps(&status_payload(None, Some(decision))),
        r#"{"operator_controls": {"values": {}}, "fee_decision": {"adjusted": 0}}"#
    );
}

/// py: `... if hasattr(fee_controller, "get_last_decision_summary") else {}`.
#[test]
fn status_publishes_an_empty_fee_decision_when_no_summary_exists() {
    assert_eq!(
        dumps(&status_payload(None, None)),
        r#"{"operator_controls": {"values": {}}, "fee_decision": {}}"#
    );
}

/// Stated as one test because it is one decision: the two producers
/// treat missing config OPPOSITELY, and a port that handled absence
/// uniformly would be wrong about exactly one of them.
#[test]
fn status_and_fee_bounds_disagree_about_a_missing_config_on_purpose() {
    assert!(
        matches!(fee_bounds_decision(None), PublishDecision::Withhold(_)),
        "fee-bounds withholds: zeros would read as a real operator floor"
    );
    assert!(
        dumps(&status_payload(None, None)).contains(r#""values": {}"#),
        "status publishes anyway: silence cannot say 'alive, holding no config'"
    );
}

// =====================================================================
// segment-observations: withheld twice
// =====================================================================

/// py `if store is None or not observer_member_id: return False`. The
/// snapshot is stamped with the observer's own member id; publishing it
/// unattributed would put an anonymous blob under a key whose consumers
/// key off that id.
#[test]
fn an_unnamed_observer_withholds_the_segment_publish() {
    assert_eq!(
        segment_publish_is_allowed("", 5),
        Err(Withheld::NoObserverIdentity)
    );
}

/// py `if not snapshot.get("segment_observations"): return False`. An
/// empty export is not news -- republishing it would overwrite a
/// still-useful previous snapshot with nothing.
#[test]
fn an_empty_observation_set_withholds_the_segment_publish() {
    assert_eq!(
        segment_publish_is_allowed("02aabb", 0),
        Err(Withheld::NoObservations)
    );
}

#[test]
fn a_named_observer_with_observations_may_publish() {
    assert!(segment_publish_is_allowed("02aabb", 1).is_ok());
}

/// Both guards fail at once. The missing identity is reported, because
/// it is the one the operator must fix -- an unnamed observer will keep
/// withholding no matter how many observations arrive.
#[test]
fn an_unnamed_observer_with_no_observations_reports_the_identity_first() {
    assert_eq!(
        segment_publish_is_allowed("", 0),
        Err(Withheld::NoObserverIdentity)
    );
}

// =====================================================================
// vocabulary
// =====================================================================

#[test]
fn every_withheld_reason_carries_a_distinct_actionable_code() {
    let codes = [
        Withheld::NoConfigSnapshot.code(),
        Withheld::NoObserverIdentity.code(),
        Withheld::NoObservations.code(),
    ];
    let unique: std::collections::BTreeSet<_> = codes.iter().collect();
    assert_eq!(
        unique.len(),
        codes.len(),
        "codes must not collide: {codes:?}"
    );
    for code in codes {
        assert!(code.starts_with("datastore_withheld_"), "{code}");
    }
}
