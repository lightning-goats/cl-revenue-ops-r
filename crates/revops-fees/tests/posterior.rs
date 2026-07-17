//! Fixture-driven parity tests for posterior recompute + DTS discounting
//! (`revops_fees::thompson::recompute`), pinned against the real Python
//! `GaussianThompsonState` oracle (`tools/port/gen_fees_fixtures.py
//! {posterior,discount}` in the port worktree).
//!
//! Every float comparison goes through `py_repr` — bit-for-bit string
//! equality, never epsilon (Global Constraints: exact float parity).

use revops_econ::pyfloat::py_repr;
use revops_fees::mat3::{M3, V3};
use revops_fees::thompson::recompute::{
    apply_dts_discount, earning_region_fee, effective_positive_rate_ref, positive_revenue_mass,
    recompute_posterior_core, recompute_posterior_legacy, BIAS_DECAY_HOURS, BIAS_MIN_WEIGHT,
    CTX_CONFIDENCE_COUNT, CTX_OFFSET_CAP_FRAC, CTX_PRECISION_DECAY, DECAY_HOURS,
    DISCOUNT_WEIGHT_FLOOR, MAX_OBSERVATIONS, MEANINGFUL_GAP_EMA_ALPHA, MIN_OBSERVATIONS,
    MIN_PRECISION, NUDGE_DEDUP_TOLERANCE, POSITIVE_RATE_EMA_ALPHA,
    POSITIVE_RATE_REF_HALF_LIFE_HOURS, REL_MIN_STD_FRAC, SECONDARY_EXPLORE_BOOST,
    SUPPORTED_CEILING_FLOOR_ESCAPE, SUPPORTED_CEILING_HEADROOM, SUPPORTED_CEILING_MASS_QUANTILE,
    SUPPORTED_CEILING_MIN_WEIGHT, TRICKLE_RESET_FRAC, UPWARD_PROBE_INTERVAL_HOURS,
    UPWARD_PROBE_MIN_STD, UPWARD_PROBE_STRETCH, ZERO_PROBE_FLOOR_FRAC, ZERO_PROBE_STEP_FRAC,
    ZERO_REGIME_ANCHOR_HALF_LIFE_HOURS, ZERO_REGIME_REL_STD, ZERO_REGIME_STREAK_OVERRIDE,
    ZERO_REVENUE_STREAK_THRESHOLD,
};
use revops_fees::thompson::{
    GaussianThompsonState, Observation, CONGESTION_OBS_FLAG, EXPLORATION_BOOST_MAX,
    EXPLORATION_BOOST_MIN, MAX_BIAS_NUDGES, MIN_STD, WEIGHT_SCHEME, ZERO_PROBE_FLAG,
    ZERO_REVENUE_WEIGHT_FACTOR,
};
use serde_json::Value;
use std::path::PathBuf;

fn fixture_path(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("../../fixtures/fees/{rel}"))
}

fn load(rel: &str) -> Value {
    let path = fixture_path(rel);
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

fn parse_f(v: &Value) -> f64 {
    v.as_str().expect("repr string").parse().expect("parse f64")
}

fn parse_v3(v: &Value) -> V3 {
    let xs = v.as_array().expect("3 elems");
    [parse_f(&xs[0]), parse_f(&xs[1]), parse_f(&xs[2])]
}

fn parse_m3(v: &Value) -> M3 {
    let rows = v.as_array().expect("3 rows");
    let mut m = [[0.0f64; 3]; 3];
    for (i, row) in rows.iter().enumerate() {
        for (j, x) in row.as_array().expect("3 cols").iter().enumerate() {
            m[i][j] = parse_f(x);
        }
    }
    m
}

/// Parse a dumped `[fee_repr, rev_repr, weight_repr, ts, time_bucket, flag?]`
/// observation array into an `Observation`.
fn parse_obs(v: &Value) -> Observation {
    let a = v.as_array().expect("observation array");
    let fee = parse_f(&a[0]);
    let revenue_rate = parse_f(&a[1]);
    let weight = parse_f(&a[2]);
    let ts = a[3].as_i64().expect("ts int");
    let time_bucket = a[4].as_str().expect("time_bucket str").to_string();
    let flag = a.get(5).and_then(|v| v.as_str()).map(|s| s.to_string());
    Observation {
        fee,
        // Serialization typing is irrelevant to recompute (it only reads
        // the f64); fixtures carry canonical int-typed fees.
        fee_is_int: true,
        revenue_rate,
        weight,
        ts,
        time_bucket,
        flag,
        extra: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// posterior/recompute.json: _recompute_posterior_core branch coverage.
// ---------------------------------------------------------------------------

#[test]
fn recompute_posterior_core_matches_python_oracle() {
    let doc = load("posterior/recompute.json");
    let now = doc["now"].as_i64().expect("now");
    let cases = doc["cases"].as_array().expect("cases array");
    assert!(
        cases.len() >= 15,
        "expected substantial branch coverage, got {}",
        cases.len()
    );

    for case in cases {
        let name = case["name"].as_str().expect("name");
        let input = &case["input"];
        let expected = &case["expected"];

        let mut state = GaussianThompsonState {
            prior_mean_fee: parse_f(&input["prior_mean_fee"]),
            prior_std_fee: parse_f(&input["prior_std_fee"]),
            prior_coeffs: parse_v3(&input["prior_coeffs"]),
            prior_precision: parse_m3(&input["prior_precision"]),
            noise_variance: parse_f(&input["noise_variance"]),
            zero_revenue_streak: input["zero_revenue_streak"].as_i64().expect("zrs"),
            zero_run_start_fee: parse_f(&input["zero_run_start_fee"]),
            zero_run_start_ts: input["zero_run_start_ts"].as_i64().expect("zrst"),
            observations: input["observations"]
                .as_array()
                .expect("observations")
                .iter()
                .map(parse_obs)
                .collect(),
            ..GaussianThompsonState::default()
        };

        recompute_posterior_core(&mut state, now);

        assert_eq!(
            py_repr(state.posterior_mean),
            expected["posterior_mean"].as_str().unwrap(),
            "{name}: posterior_mean"
        );
        assert_eq!(
            py_repr(state.posterior_std),
            expected["posterior_std"].as_str().unwrap(),
            "{name}: posterior_std"
        );
        for i in 0..3 {
            assert_eq!(
                py_repr(state.posterior_coeffs[i]),
                expected["posterior_coeffs"][i].as_str().unwrap(),
                "{name}: posterior_coeffs[{i}]"
            );
        }
        for i in 0..3 {
            for j in 0..3 {
                assert_eq!(
                    py_repr(state.posterior_precision[i][j]),
                    expected["posterior_precision"][i][j].as_str().unwrap(),
                    "{name}: posterior_precision[{i}][{j}]"
                );
            }
        }
        assert_eq!(
            py_repr(state.noise_variance),
            expected["noise_variance"].as_str().unwrap(),
            "{name}: noise_variance"
        );
        assert_eq!(
            py_repr(state.charged_fee_mean),
            expected["charged_fee_mean"].as_str().unwrap(),
            "{name}: charged_fee_mean"
        );
        assert_eq!(
            py_repr(state.last_fee_min),
            expected["last_fee_min"].as_str().unwrap(),
            "{name}: last_fee_min"
        );
        assert_eq!(
            py_repr(state.last_fee_max),
            expected["last_fee_max"].as_str().unwrap(),
            "{name}: last_fee_max"
        );
    }
}

// ---------------------------------------------------------------------------
// posterior/helpers.json: _positive_revenue_mass / _earning_region_fee /
// _effective_positive_rate_ref direct pins.
// ---------------------------------------------------------------------------

#[test]
fn positive_revenue_mass_and_earning_region_fee_match_python_oracle() {
    let doc = load("posterior/helpers.json");
    let cases = doc["positive_revenue_mass_cases"]
        .as_array()
        .expect("cases");
    assert!(cases.len() >= 4);

    for case in cases {
        let name = case["name"].as_str().expect("name");
        let now = case["now"].as_i64().expect("now");
        let observations: Vec<Observation> = case["observations"]
            .as_array()
            .expect("observations")
            .iter()
            .map(parse_obs)
            .collect();
        let mut state = GaussianThompsonState {
            observations,
            ..GaussianThompsonState::default()
        };

        let masses = positive_revenue_mass(&state, now);
        let expected_masses = case["positive_revenue_mass"].as_array().expect("masses");
        assert_eq!(masses.len(), expected_masses.len(), "{name}: mass count");
        for (i, (fee, mass)) in masses.iter().enumerate() {
            assert_eq!(
                py_repr(*fee),
                expected_masses[i][0].as_str().unwrap(),
                "{name}: mass[{i}].fee"
            );
            assert_eq!(
                py_repr(*mass),
                expected_masses[i][1].as_str().unwrap(),
                "{name}: mass[{i}].mass"
            );
        }

        let earning = earning_region_fee(&state, now);
        match &case["earning_region_fee"] {
            Value::Null => assert!(
                earning.is_none(),
                "{name}: expected None earning_region_fee"
            ),
            Value::String(s) => {
                assert_eq!(
                    py_repr(earning.expect("Some")),
                    *s,
                    "{name}: earning_region_fee"
                );
            }
            other => panic!("unexpected earning_region_fee fixture value: {other:?}"),
        }

        // state is otherwise unused after this point in the case, but keep
        // it alive so field access above type-checks without a warning.
        let _ = &mut state;
    }
}

#[test]
fn effective_positive_rate_ref_matches_python_oracle() {
    let doc = load("posterior/helpers.json");
    let cases = doc["effective_positive_rate_ref_cases"]
        .as_array()
        .expect("cases");
    assert!(cases.len() >= 4);

    for case in cases {
        let name = case["name"].as_str().expect("name");
        let now = case["now"].as_i64().expect("now");
        let state = GaussianThompsonState {
            positive_rate_ref: parse_f(&case["positive_rate_ref"]),
            positive_rate_ref_ts: case["positive_rate_ref_ts"].as_i64().expect("ts"),
            ..GaussianThompsonState::default()
        };
        let actual = effective_positive_rate_ref(&state, now);
        assert_eq!(
            py_repr(actual),
            case["expected"].as_str().unwrap(),
            "{name}"
        );
    }
}

// ---------------------------------------------------------------------------
// discount/sequences.json: apply_dts_discount order-of-operations proof.
// ---------------------------------------------------------------------------

#[test]
fn discount_sequences_match_python_oracle_at_every_step() {
    let doc = load("discount/sequences.json");
    let now = doc["now"].as_i64().expect("now");
    let sequences = doc["sequences"].as_array().expect("sequences");
    assert!(sequences.len() >= 4);

    for seq in sequences {
        let name = seq["name"].as_str().expect("name");
        let observations: Vec<Observation> = seq["observations"]
            .as_array()
            .expect("observations")
            .iter()
            .map(parse_obs)
            .collect();
        let mut state = GaussianThompsonState {
            observations,
            ..GaussianThompsonState::default()
        };

        for (i, step) in seq["steps"].as_array().expect("steps").iter().enumerate() {
            let op = step["op"].as_str().expect("op");
            match op {
                "recompute" => recompute_posterior_core(&mut state, now),
                "discount" => {
                    let gamma = parse_f(&step["gamma"]);
                    apply_dts_discount(&mut state, gamma);
                }
                other => panic!("unknown op {other}"),
            }

            let expect = &step["state"];
            assert_eq!(
                py_repr(state.posterior_mean),
                expect["posterior_mean"].as_str().unwrap(),
                "{name} step {i} ({op}): posterior_mean"
            );
            assert_eq!(
                py_repr(state.posterior_std),
                expect["posterior_std"].as_str().unwrap(),
                "{name} step {i} ({op}): posterior_std"
            );
            for k in 0..3 {
                assert_eq!(
                    py_repr(state.posterior_coeffs[k]),
                    expect["posterior_coeffs"][k].as_str().unwrap(),
                    "{name} step {i} ({op}): posterior_coeffs[{k}]"
                );
            }
            for r in 0..3 {
                for c in 0..3 {
                    assert_eq!(
                        py_repr(state.posterior_precision[r][c]),
                        expect["posterior_precision"][r][c].as_str().unwrap(),
                        "{name} step {i} ({op}): posterior_precision[{r}][{c}]"
                    );
                }
            }
            assert_eq!(
                py_repr(state.noise_variance),
                expect["noise_variance"].as_str().unwrap(),
                "{name} step {i} ({op}): noise_variance"
            );
            let expected_weights = expect["observation_weights"].as_array().expect("weights");
            assert_eq!(
                state.observations.len(),
                expected_weights.len(),
                "{name} step {i}: obs count"
            );
            for (w_idx, obs) in state.observations.iter().enumerate() {
                assert_eq!(
                    py_repr(obs.weight),
                    expected_weights[w_idx].as_str().unwrap(),
                    "{name} step {i} ({op}): observation_weights[{w_idx}]"
                );
            }
        }
    }
}

#[test]
fn discount_noop_guard_outside_open_unit_interval() {
    // apply_dts_discount is a documented no-op for gamma outside (0, 1) —
    // exercised directly (independent of fixtures) since it's a pure guard.
    let mut state = GaussianThompsonState {
        observations: vec![Observation::new(100.0, 50.0, 1.0, 0, "all")],
        ..GaussianThompsonState::default()
    };
    let before = state.clone();
    for gamma in [1.0, 0.0, -0.5, 1.5, f64::NAN] {
        apply_dts_discount(&mut state, gamma);
        assert_eq!(state.posterior_std, before.posterior_std, "gamma={gamma}");
        assert_eq!(
            state.posterior_precision, before.posterior_precision,
            "gamma={gamma}"
        );
        assert_eq!(
            state.observations[0].weight, before.observations[0].weight,
            "gamma={gamma}"
        );
    }
}

// ---------------------------------------------------------------------------
// Legacy fallback: direct behavioral checks not already covered via the
// core-recompute fixture branches (empty-state shortcut, explicit list).
// ---------------------------------------------------------------------------

#[test]
fn legacy_fallback_empty_state_resets_to_prior_when_weighted_obs_is_none() {
    let mut state = GaussianThompsonState {
        prior_mean_fee: 321.0,
        prior_std_fee: 45.0,
        observations: Vec::new(),
        ..GaussianThompsonState::default()
    };
    recompute_posterior_legacy(&mut state, None, 1_752_400_000);
    assert_eq!(state.posterior_mean, 321.0);
    assert_eq!(state.posterior_std, 45.0);
}

// ---------------------------------------------------------------------------
// Pinned-constants test: every constant transcribed from
// `fee_controller.py:258-383` plus MIN_PRECISION (1663) and
// DISCOUNT_WEIGHT_FLOOR (1670). Values must never drift silently.
// ---------------------------------------------------------------------------

#[test]
fn pinned_constants_match_python_source() {
    assert_eq!(MAX_OBSERVATIONS, 200);
    assert_eq!(DECAY_HOURS, 168.0);
    assert_eq!(MIN_OBSERVATIONS, 5);
    assert_eq!(MIN_STD, 10.0);
    assert_eq!(WEIGHT_SCHEME, "exposure_v2");
    assert_eq!(ZERO_REVENUE_WEIGHT_FACTOR, 0.15);
    assert_eq!(TRICKLE_RESET_FRAC, 0.10);
    assert_eq!(POSITIVE_RATE_EMA_ALPHA, 0.2);
    assert_eq!(POSITIVE_RATE_REF_HALF_LIFE_HOURS, 168.0);
    assert_eq!(MEANINGFUL_GAP_EMA_ALPHA, 0.3);
    assert_eq!(UPWARD_PROBE_STRETCH, 1.25);
    assert_eq!(UPWARD_PROBE_INTERVAL_HOURS, 24.0);
    assert_eq!(UPWARD_PROBE_MIN_STD, 60.0);
    assert_eq!(SUPPORTED_CEILING_HEADROOM, 1.25);
    assert_eq!(SUPPORTED_CEILING_MASS_QUANTILE, 0.90);
    assert_eq!(SUPPORTED_CEILING_MIN_WEIGHT, 1e-3);
    assert_eq!(SUPPORTED_CEILING_FLOOR_ESCAPE, 2.0);
    assert_eq!(CONGESTION_OBS_FLAG, "congestion");
    assert_eq!(REL_MIN_STD_FRAC, 0.04);
    assert_eq!(ZERO_REVENUE_STREAK_THRESHOLD, 4);
    assert_eq!(ZERO_PROBE_STEP_FRAC, 0.9);
    assert_eq!(ZERO_PROBE_FLOOR_FRAC, 0.3);
    assert_eq!(ZERO_PROBE_FLAG, "zero_probe");
    assert_eq!(ZERO_REGIME_REL_STD, 0.15);
    assert_eq!(ZERO_REGIME_STREAK_OVERRIDE, 24);
    assert_eq!(ZERO_REGIME_ANCHOR_HALF_LIFE_HOURS, 24.0);
    assert_eq!(SECONDARY_EXPLORE_BOOST, 1.25);
    assert_eq!(MAX_BIAS_NUDGES, 50);
    assert_eq!(BIAS_DECAY_HOURS, 24.0);
    assert_eq!(BIAS_MIN_WEIGHT, 1e-3);
    assert_eq!(NUDGE_DEDUP_TOLERANCE, 0.05);
    assert_eq!(CTX_OFFSET_CAP_FRAC, 0.20);
    assert_eq!(CTX_CONFIDENCE_COUNT, 10.0);
    assert_eq!(CTX_PRECISION_DECAY, 0.98);
    assert_eq!(EXPLORATION_BOOST_MIN, 0.75);
    assert_eq!(EXPLORATION_BOOST_MAX, 2.0);
    assert_eq!(MIN_PRECISION, 0.000025);
    assert_eq!(DISCOUNT_WEIGHT_FLOOR, 0.05);
}
