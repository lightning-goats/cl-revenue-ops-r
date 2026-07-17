//! Fixture-driven parity tests for Thompson sampling paths
//! (`revops_fees::thompson::sampling`): seeded end-to-end draw scenarios
//! pinning the SAMPLED FEE bit-for-bit across the sparse-prior /
//! polynomial-concave / Cholesky-fallback / non-concave-Gaussian-fallback /
//! contextual-offset paths, against the real Python oracle
//! (`tools/port/gen_fees_fixtures.py sampling` in the port worktree).
//!
//! Draw-count parity is THE contract here (Phase 4 plan, RNG trap
//! section): after every sampled fee the fixture pins the NEXT
//! `gauss(0, 1)` and `random()` from the SAME stream — one missed or extra
//! draw (or a dropped `gauss_next` cache) desyncs those pins.

use revops_econ::pyfloat::py_repr;
use revops_fees::mat3::{M3, V3};
use revops_fees::pyrand::PyRandom;
use revops_fees::thompson::dynamics::posterior_bias_shift;
use revops_fees::thompson::sampling::{get_exploitation_fee, sample_fee, sample_fee_contextual};
use revops_fees::thompson::{CtxPosterior, GaussianThompsonState, Observation, MIN_STD};
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

fn parse_obs(v: &Value) -> Observation {
    let a = v.as_array().expect("observation array");
    Observation {
        fee: parse_f(&a[0]),
        fee_is_int: true,
        revenue_rate: parse_f(&a[1]),
        weight: parse_f(&a[2]),
        ts: a[3].as_i64().expect("ts int"),
        time_bucket: a[4].as_str().expect("time_bucket str").to_string(),
        flag: a.get(5).and_then(|v| v.as_str()).map(|s| s.to_string()),
        extra: Vec::new(),
    }
}

fn parse_ctx(v: &Value) -> CtxPosterior {
    let a = v.as_array().expect("ctx array");
    if a.len() == 3 {
        let std = parse_f(&a[1]);
        CtxPosterior {
            mean: parse_f(&a[0]),
            precision: 1.0 / (std * std).max(MIN_STD * MIN_STD),
            count: a[2].as_i64().expect("count"),
            last_update: 0,
            was_legacy_3tuple: true,
        }
    } else {
        CtxPosterior {
            mean: parse_f(&a[0]),
            precision: parse_f(&a[1]),
            count: a[2].as_i64().expect("count"),
            last_update: a[3].as_i64().expect("last_update"),
            was_legacy_3tuple: false,
        }
    }
}

fn build_state(spec: &Value) -> GaussianThompsonState {
    let mut state = GaussianThompsonState::default();
    let obj = spec.as_object().expect("state object");
    for (k, v) in obj {
        match k.as_str() {
            "zero_revenue_streak" => state.zero_revenue_streak = v.as_i64().unwrap(),
            "prior_mean_fee" => state.prior_mean_fee = parse_f(v),
            "prior_std_fee" => state.prior_std_fee = parse_f(v),
            "posterior_mean" => state.posterior_mean = parse_f(v),
            "posterior_std" => state.posterior_std = parse_f(v),
            "charged_fee_mean" => state.charged_fee_mean = parse_f(v),
            "last_fee_min" => state.last_fee_min = parse_f(v),
            "last_fee_max" => state.last_fee_max = parse_f(v),
            "posterior_coeffs" => state.posterior_coeffs = parse_v3(v),
            "posterior_precision" => state.posterior_precision = parse_m3(v),
            "observations" => {
                state.observations = v.as_array().unwrap().iter().map(parse_obs).collect()
            }
            "posterior_bias" => {
                state.posterior_bias = v
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|e| {
                        let a = e.as_array().unwrap();
                        (parse_f(&a[0]), parse_f(&a[1]), a[2].as_i64().unwrap())
                    })
                    .collect()
            }
            "contextual_posteriors" => {
                state.contextual_posteriors = v
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|pair| {
                        let p = pair.as_array().expect("ctx pair");
                        (p[0].as_str().unwrap().to_string(), parse_ctx(&p[1]))
                    })
                    .collect()
            }
            other => panic!("unknown state key {other}"),
        }
    }
    state
}

// ---------------------------------------------------------------------------
// sampling/draws.json: seeded end-to-end draw scenarios.
// ---------------------------------------------------------------------------

#[test]
fn seeded_draw_scenarios_match_python_bit_for_bit() {
    let doc = load("sampling/draws.json");
    let cases = doc["cases"].as_array().expect("cases");
    assert!(
        cases.len() >= 30,
        "expected >= 30 draw scenarios, got {}",
        cases.len()
    );

    // Per-branch coverage guard: every RNG-consumption shape must appear.
    let mut branch_counts: std::collections::HashMap<&str, usize> =
        std::collections::HashMap::new();

    for case in cases {
        let name = case["name"].as_str().expect("name");
        let now = case["now"].as_i64().expect("now");
        let seed = case["seed"].as_u64().expect("seed");
        let mut state = build_state(&case["state"]);
        let call = &case["call"];
        let expected = &case["expected"];

        *branch_counts
            .entry(expected["branch"].as_str().expect("branch"))
            .or_default() += 1;

        let floor = call["floor"].as_i64().unwrap();
        let ceiling = call["ceiling"].as_i64().unwrap();
        let multiplier = match &call["exploration_multiplier"] {
            Value::Null => None,
            v => Some(parse_f(v)),
        };

        let mut rng = PyRandom::seed_from_u64(seed);
        let fee = match call["fn"].as_str().unwrap() {
            "sample_fee" => sample_fee(&mut state, floor, ceiling, multiplier, &mut rng, now),
            "sample_fee_contextual" => sample_fee_contextual(
                &mut state,
                call["context_key"].as_str().unwrap(),
                floor,
                ceiling,
                multiplier,
                &mut rng,
                now,
            ),
            other => panic!("unknown fn {other}"),
        };

        assert_eq!(fee, expected["fee"].as_i64().unwrap(), "{name}: sampled fee");
        assert_eq!(
            state.last_sampled_fee,
            expected["last_sampled_fee"].as_i64().unwrap(),
            "{name}: last_sampled_fee"
        );
        assert_eq!(
            state.last_sample_time,
            expected["last_sample_time"].as_i64().unwrap(),
            "{name}: last_sample_time"
        );

        // Draw-count parity: the NEXT gauss (consumes/refills the cached
        // Box-Muller pair exactly as CPython) and the NEXT random() must
        // both match — this fails if the sample path consumed one draw too
        // many or too few, or mishandled the gauss_next cache.
        let post_gauss = rng.gauss(0.0, 1.0);
        assert_eq!(
            py_repr(post_gauss),
            expected["post_gauss"].as_str().unwrap(),
            "{name}: post-sample gauss stream pin (draw-count parity)"
        );
        let post_random = rng.random();
        assert_eq!(
            py_repr(post_random),
            expected["post_random"].as_str().unwrap(),
            "{name}: post-sample random stream pin (draw-count parity)"
        );
    }

    for branch in [
        "sparse_prior",
        "poly_concave",
        "poly_cholesky_fallback",
        "non_concave_gauss_fallback",
        "gauss_fallback_no_poly_draws",
        "ctx_offset",
        "ctx_passthrough",
    ] {
        assert!(
            branch_counts.get(branch).copied().unwrap_or(0) >= 1,
            "missing fixture coverage for branch {branch}: {branch_counts:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// sampling/shift.json: _posterior_bias_shift direct pins.
// ---------------------------------------------------------------------------

#[test]
fn posterior_bias_shift_matches_python_oracle() {
    let doc = load("sampling/shift.json");
    let cases = doc["cases"].as_array().expect("cases");
    assert!(cases.len() >= 6);

    for case in cases {
        let name = case["name"].as_str().unwrap();
        let now = case["now"].as_i64().unwrap();
        let state = GaussianThompsonState {
            posterior_bias: case["posterior_bias"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| {
                    let a = e.as_array().unwrap();
                    (parse_f(&a[0]), parse_f(&a[1]), a[2].as_i64().unwrap())
                })
                .collect(),
            ..GaussianThompsonState::default()
        };
        let base = parse_f(&case["base"]);
        assert_eq!(
            py_repr(posterior_bias_shift(&state, base, now)),
            case["expected"].as_str().unwrap(),
            "{name}"
        );
    }
}

// ---------------------------------------------------------------------------
// get_exploitation_fee: int() truncation toward zero (py 1632-1634).
// ---------------------------------------------------------------------------

#[test]
fn get_exploitation_fee_truncates_toward_zero() {
    for (mean, expected) in [
        (200.0, 200),
        (200.9, 200),
        (199.0000001, 199),
        (0.9, 0),
        (-0.5, 0),
        (-1.5, -1),
    ] {
        let state = GaussianThompsonState {
            posterior_mean: mean,
            ..GaussianThompsonState::default()
        };
        assert_eq!(get_exploitation_fee(&state), expected, "mean={mean}");
    }
}
