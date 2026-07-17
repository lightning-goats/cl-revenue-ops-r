//! Fixture-driven parity tests for Thompson dynamics
//! (`revops_fees::thompson::dynamics`): `update_posterior` state-machine
//! sequences, `supported_fee_ceiling`/`maybe_upward_probe_cap` gate
//! matrices, contextual-posterior updates (incl. the stable-sort prune
//! order trap), posterior nudges + the bias re-apply path, and the pure
//! failed-forward math — all pinned against the real Python
//! `GaussianThompsonState` oracle (`tools/port/gen_fees_fixtures.py
//! {update,ceiling}` in the port worktree).
//!
//! Every float comparison goes through `py_repr` — bit-for-bit string
//! equality, never epsilon (Global Constraints: exact float parity).

use revops_econ::pyfloat::py_repr;
use revops_fees::mat3::{M3, V3};
use revops_fees::thompson::dynamics::{
    apply_vegas_adjustment, consume_upward_probe, failed_forward_implied_fee,
    failed_forward_nudge_weight, is_fee_relevant_failure, is_meaningful_rate,
    maybe_upward_probe_cap, real_observation_count, record_posterior_nudge, supported_fee_ceiling,
    update_contextual, update_posterior, FEE_RELEVANT_FAILCODES,
};
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

/// Parse a dumped contextual-posterior value: `[mean, precision, count,
/// last_update]` (current 4-tuple) or `[mean, std, count]` (legacy 3-tuple,
/// converted with from_dict's formula — the identical conversion
/// `update_contextual` applies at runtime, py 1049-1052).
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

fn parse_bias(v: &Value) -> Vec<(f64, f64, i64)> {
    v.as_array()
        .expect("bias array")
        .iter()
        .map(|e| {
            let a = e.as_array().expect("bias entry");
            (parse_f(&a[0]), parse_f(&a[1]), a[2].as_i64().expect("ts"))
        })
        .collect()
}

/// Build a `GaussianThompsonState` from a fixture `initial`/`state` object
/// (all keys optional; missing keys keep the Python dataclass defaults,
/// which `GaussianThompsonState::default()` mirrors).
fn build_state(initial: &Value) -> GaussianThompsonState {
    let mut state = GaussianThompsonState::default();
    if initial.is_null() {
        return state;
    }
    let obj = initial.as_object().expect("initial object");
    for (k, v) in obj {
        match k.as_str() {
            "zero_revenue_streak" => state.zero_revenue_streak = v.as_i64().unwrap(),
            "zero_run_start_ts" => state.zero_run_start_ts = v.as_i64().unwrap(),
            "positive_rate_ref_ts" => state.positive_rate_ref_ts = v.as_i64().unwrap(),
            "last_meaningful_ts" => state.last_meaningful_ts = v.as_i64().unwrap(),
            "last_upward_probe_ts" => state.last_upward_probe_ts = v.as_i64().unwrap(),
            "prior_mean_fee" => state.prior_mean_fee = parse_f(v),
            "prior_std_fee" => state.prior_std_fee = parse_f(v),
            "posterior_mean" => state.posterior_mean = parse_f(v),
            "posterior_std" => state.posterior_std = parse_f(v),
            "charged_fee_mean" => state.charged_fee_mean = parse_f(v),
            "noise_variance" => state.noise_variance = parse_f(v),
            "zero_run_start_fee" => state.zero_run_start_fee = parse_f(v),
            "positive_rate_ref" => state.positive_rate_ref = parse_f(v),
            "meaningful_gap_ema_hours" => state.meaningful_gap_ema_hours = parse_f(v),
            "last_fee_min" => state.last_fee_min = parse_f(v),
            "last_fee_max" => state.last_fee_max = parse_f(v),
            "posterior_coeffs" => state.posterior_coeffs = parse_v3(v),
            "posterior_precision" => state.posterior_precision = parse_m3(v),
            "observations" => {
                state.observations = v.as_array().unwrap().iter().map(parse_obs).collect()
            }
            "posterior_bias" => state.posterior_bias = parse_bias(v),
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
            other => panic!("unknown initial key {other}"),
        }
    }
    state
}

fn assert_ctx_matches(actual: &CtxPosterior, expected: &Value, label: &str) {
    let a = expected.as_array().expect("ctx array");
    assert_eq!(
        py_repr(actual.mean),
        a[0].as_str().unwrap(),
        "{label}: ctx mean"
    );
    if a.len() == 3 {
        // Untouched legacy 3-tuple: Python still stores (mean, std, count);
        // the Rust struct stores the runtime-converted precision plus the
        // legacy flag so serde can re-emit the original shape.
        assert!(actual.was_legacy_3tuple, "{label}: legacy flag");
        let std = parse_f(&a[1]);
        assert_eq!(
            py_repr(actual.precision),
            py_repr(1.0 / (std * std).max(MIN_STD * MIN_STD)),
            "{label}: legacy ctx precision"
        );
        assert_eq!(actual.count, a[2].as_i64().unwrap(), "{label}: ctx count");
        assert_eq!(actual.last_update, 0, "{label}: legacy ctx last_update");
    } else {
        assert!(!actual.was_legacy_3tuple, "{label}: legacy flag cleared");
        assert_eq!(
            py_repr(actual.precision),
            a[1].as_str().unwrap(),
            "{label}: ctx precision"
        );
        assert_eq!(actual.count, a[2].as_i64().unwrap(), "{label}: ctx count");
        assert_eq!(
            actual.last_update,
            a[3].as_i64().unwrap(),
            "{label}: ctx last_update"
        );
    }
}

fn assert_snapshot(state: &GaussianThompsonState, expected: &Value, label: &str) {
    // Observations: full tuple pins.
    let exp_obs = expected["observations"].as_array().expect("observations");
    assert_eq!(
        state.observations.len(),
        exp_obs.len(),
        "{label}: obs count"
    );
    for (i, (actual, exp)) in state.observations.iter().zip(exp_obs).enumerate() {
        let a = exp.as_array().unwrap();
        assert_eq!(
            py_repr(actual.fee),
            a[0].as_str().unwrap(),
            "{label}: obs[{i}].fee"
        );
        assert_eq!(
            py_repr(actual.revenue_rate),
            a[1].as_str().unwrap(),
            "{label}: obs[{i}].revenue_rate"
        );
        assert_eq!(
            py_repr(actual.weight),
            a[2].as_str().unwrap(),
            "{label}: obs[{i}].weight"
        );
        assert_eq!(actual.ts, a[3].as_i64().unwrap(), "{label}: obs[{i}].ts");
        assert_eq!(
            actual.time_bucket,
            a[4].as_str().unwrap(),
            "{label}: obs[{i}].time_bucket"
        );
        assert_eq!(
            actual.flag.as_deref(),
            a.get(5).and_then(|v| v.as_str()),
            "{label}: obs[{i}].flag"
        );
    }

    // Streak / reference / cadence fields.
    assert_eq!(
        state.zero_revenue_streak,
        expected["zero_revenue_streak"].as_i64().unwrap(),
        "{label}: zero_revenue_streak"
    );
    assert_eq!(
        py_repr(state.zero_run_start_fee),
        expected["zero_run_start_fee"].as_str().unwrap(),
        "{label}: zero_run_start_fee"
    );
    assert_eq!(
        state.zero_run_start_ts,
        expected["zero_run_start_ts"].as_i64().unwrap(),
        "{label}: zero_run_start_ts"
    );
    assert_eq!(
        py_repr(state.positive_rate_ref),
        expected["positive_rate_ref"].as_str().unwrap(),
        "{label}: positive_rate_ref"
    );
    assert_eq!(
        state.positive_rate_ref_ts,
        expected["positive_rate_ref_ts"].as_i64().unwrap(),
        "{label}: positive_rate_ref_ts"
    );
    assert_eq!(
        py_repr(state.meaningful_gap_ema_hours),
        expected["meaningful_gap_ema_hours"].as_str().unwrap(),
        "{label}: meaningful_gap_ema_hours"
    );
    assert_eq!(
        state.last_meaningful_ts,
        expected["last_meaningful_ts"].as_i64().unwrap(),
        "{label}: last_meaningful_ts"
    );
    assert_eq!(
        state.last_upward_probe_ts,
        expected["last_upward_probe_ts"].as_i64().unwrap(),
        "{label}: last_upward_probe_ts"
    );

    // Posterior fields.
    assert_eq!(
        py_repr(state.posterior_mean),
        expected["posterior_mean"].as_str().unwrap(),
        "{label}: posterior_mean"
    );
    assert_eq!(
        py_repr(state.posterior_std),
        expected["posterior_std"].as_str().unwrap(),
        "{label}: posterior_std"
    );
    for i in 0..3 {
        assert_eq!(
            py_repr(state.posterior_coeffs[i]),
            expected["posterior_coeffs"][i].as_str().unwrap(),
            "{label}: posterior_coeffs[{i}]"
        );
        for j in 0..3 {
            assert_eq!(
                py_repr(state.posterior_precision[i][j]),
                expected["posterior_precision"][i][j].as_str().unwrap(),
                "{label}: posterior_precision[{i}][{j}]"
            );
        }
    }
    assert_eq!(
        py_repr(state.noise_variance),
        expected["noise_variance"].as_str().unwrap(),
        "{label}: noise_variance"
    );
    assert_eq!(
        py_repr(state.charged_fee_mean),
        expected["charged_fee_mean"].as_str().unwrap(),
        "{label}: charged_fee_mean"
    );
    assert_eq!(
        py_repr(state.last_fee_min),
        expected["last_fee_min"].as_str().unwrap(),
        "{label}: last_fee_min"
    );
    assert_eq!(
        py_repr(state.last_fee_max),
        expected["last_fee_max"].as_str().unwrap(),
        "{label}: last_fee_max"
    );

    // Durable nudges.
    let exp_bias = parse_bias(&expected["posterior_bias"]);
    assert_eq!(
        state.posterior_bias.len(),
        exp_bias.len(),
        "{label}: bias count"
    );
    for (i, ((at, aw, ats), (et, ew, ets))) in
        state.posterior_bias.iter().zip(&exp_bias).enumerate()
    {
        assert_eq!(py_repr(*at), py_repr(*et), "{label}: bias[{i}].target");
        assert_eq!(py_repr(*aw), py_repr(*ew), "{label}: bias[{i}].weight");
        assert_eq!(ats, ets, "{label}: bias[{i}].ts");
    }

    // Contextual posteriors: key ORDER is load-bearing (insertion-ordered
    // map; the prune reorders to count-desc).
    let exp_ctx = expected["contextual_posteriors"].as_array().unwrap();
    assert_eq!(
        state.contextual_posteriors.len(),
        exp_ctx.len(),
        "{label}: ctx count"
    );
    for (i, ((key, actual), pair)) in state.contextual_posteriors.iter().zip(exp_ctx).enumerate() {
        let p = pair.as_array().unwrap();
        assert_eq!(key, p[0].as_str().unwrap(), "{label}: ctx[{i}] key order");
        assert_ctx_matches(actual, &p[1], &format!("{label}: ctx[{i}] ({key})"));
    }
}

// ---------------------------------------------------------------------------
// update/sequences.json: multi-step dynamics scenarios with full state
// snapshots + read-only checks after every step.
// ---------------------------------------------------------------------------

#[test]
fn update_sequences_match_python_oracle_at_every_step() {
    let doc = load("update/sequences.json");
    let sequences = doc["sequences"].as_array().expect("sequences");
    assert!(
        sequences.len() >= 20,
        "expected >= 20 update scenarios, got {}",
        sequences.len()
    );

    for seq in sequences {
        let name = seq["name"].as_str().expect("name");
        let mut state = build_state(&seq["initial"]);

        for (i, step) in seq["steps"].as_array().expect("steps").iter().enumerate() {
            let op = step["op"].as_str().expect("op");
            let now = step["now"].as_i64().expect("now");
            let args = &step["args"];
            let label = format!("{name} step {i} ({op})");

            match op {
                "update_posterior" => update_posterior(
                    &mut state,
                    parse_f(&args["fee"]),
                    parse_f(&args["revenue_rate"]),
                    parse_f(&args["hours"]),
                    args["time_bucket"].as_str().unwrap(),
                    args["congested"].as_bool().unwrap(),
                    now,
                ),
                "nudge" => record_posterior_nudge(
                    &mut state,
                    parse_f(&args["target_fee"]),
                    parse_f(&args["weight"]),
                    now,
                ),
                "vegas" => apply_vegas_adjustment(
                    &mut state,
                    parse_f(&args["vegas_multiplier"]),
                    parse_f(&args["new_floor"]),
                    now,
                ),
                "update_contextual" => update_contextual(
                    &mut state,
                    args["context_key"].as_str().unwrap(),
                    parse_f(&args["fee"]),
                    parse_f(&args["revenue_rate"]),
                    args["time_bucket"].as_str().unwrap(),
                    now,
                ),
                "consume_upward_probe" => consume_upward_probe(&mut state, now),
                "recompute" => {
                    revops_fees::thompson::recompute::recompute_posterior(&mut state, now)
                }
                other => panic!("unknown op {other}"),
            }

            // Read-only checks.
            let checks = &step["expected"]["checks"];
            assert_eq!(
                real_observation_count(&state),
                checks["real_observation_count"].as_i64().unwrap(),
                "{label}: real_observation_count"
            );
            let floor_ppm = match &checks["ceiling_floor_ppm"] {
                Value::Null => None,
                v => Some(parse_f(v)),
            };
            let ceiling = supported_fee_ceiling(&state, now, floor_ppm);
            match &checks["supported_fee_ceiling"] {
                Value::Null => assert!(ceiling.is_none(), "{label}: ceiling None"),
                Value::String(s) => {
                    assert_eq!(
                        py_repr(ceiling.expect("Some ceiling")),
                        *s,
                        "{label}: ceiling"
                    )
                }
                other => panic!("bad ceiling fixture {other:?}"),
            }
            if let Some(rates) = checks.get("is_meaningful_rate").and_then(|v| v.as_array()) {
                for probe in rates {
                    let p = probe.as_array().unwrap();
                    let rate = parse_f(&p[0]);
                    assert_eq!(
                        is_meaningful_rate(&state, rate, now),
                        p[1].as_bool().unwrap(),
                        "{label}: is_meaningful_rate({rate})"
                    );
                }
            }

            assert_snapshot(&state, &step["expected"]["state"], &label);
        }
    }
}

// ---------------------------------------------------------------------------
// update/contextual_prune.json: the stable-sort / insertion-order prune
// trap. All-count-1 overflow drops the JUST-INSERTED key; the surviving map
// is REORDERED to count-desc (stable on ties).
// ---------------------------------------------------------------------------

#[test]
fn contextual_prune_preserves_python_stable_sort_order() {
    let doc = load("update/contextual_prune.json");
    let mut state = GaussianThompsonState::default();

    for op in doc["ops"].as_array().expect("ops") {
        update_contextual(
            &mut state,
            op["context_key"].as_str().unwrap(),
            op["fee"].as_i64().expect("fee int") as f64,
            parse_f(&op["revenue_rate"]),
            op["time_bucket"].as_str().unwrap(),
            op["now"].as_i64().unwrap(),
        );
    }

    let expected_keys: Vec<&str> = doc["expected_keys_in_order"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    let actual_keys: Vec<&str> = state
        .contextual_posteriors
        .iter()
        .map(|(k, _)| k.as_str())
        .collect();
    assert_eq!(actual_keys, expected_keys, "prune survivor order");

    for (i, pair) in doc["expected_contexts"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let p = pair.as_array().unwrap();
        let key = p[0].as_str().unwrap();
        let (_, actual) = &state.contextual_posteriors[i];
        assert_ctx_matches(actual, &p[1], &format!("prune ctx[{i}] ({key})"));
    }
}

// ---------------------------------------------------------------------------
// ceiling/ceiling.json + ceiling/probe_cap.json.
// ---------------------------------------------------------------------------

#[test]
fn supported_fee_ceiling_matches_python_oracle() {
    let doc = load("ceiling/ceiling.json");
    let cases = doc["cases"].as_array().expect("cases");
    assert!(cases.len() >= 10);

    for case in cases {
        let name = case["name"].as_str().unwrap();
        let now = case["now"].as_i64().unwrap();
        let floor_ppm = match &case["floor_ppm"] {
            Value::Null => None,
            v => Some(parse_f(v)),
        };
        let state = GaussianThompsonState {
            observations: case["observations"]
                .as_array()
                .unwrap()
                .iter()
                .map(parse_obs)
                .collect(),
            ..GaussianThompsonState::default()
        };
        let actual = supported_fee_ceiling(&state, now, floor_ppm);
        match &case["expected"] {
            Value::Null => assert!(actual.is_none(), "{name}: expected None"),
            Value::String(s) => assert_eq!(py_repr(actual.expect("Some")), *s, "{name}"),
            other => panic!("bad expected {other:?}"),
        }
    }
}

#[test]
fn maybe_upward_probe_cap_gate_matrix_matches_python_oracle() {
    let doc = load("ceiling/probe_cap.json");
    let cases = doc["cases"].as_array().expect("cases");
    assert!(cases.len() >= 12);

    for case in cases {
        let name = case["name"].as_str().unwrap();
        let state = GaussianThompsonState {
            zero_revenue_streak: case["zero_revenue_streak"].as_i64().unwrap(),
            posterior_mean: parse_f(&case["posterior_mean"]),
            posterior_std: parse_f(&case["posterior_std"]),
            last_upward_probe_ts: case["last_upward_probe_ts"].as_i64().unwrap(),
            ..GaussianThompsonState::default()
        };
        let now = case["now"].as_i64().unwrap();
        let cap = parse_f(&case["supported_cap"]);
        let actual = maybe_upward_probe_cap(&state, now, cap);
        match &case["expected"] {
            Value::Null => assert!(actual.is_none(), "{name}: expected None"),
            Value::String(s) => assert_eq!(py_repr(actual.expect("Some")), *s, "{name}"),
            other => panic!("bad expected {other:?}"),
        }
    }
}

#[test]
fn consume_upward_probe_stamps_injected_now() {
    let mut state = GaussianThompsonState::default();
    assert_eq!(state.last_upward_probe_ts, 0);
    consume_upward_probe(&mut state, 1_752_400_123);
    assert_eq!(state.last_upward_probe_ts, 1_752_400_123);
}

// ---------------------------------------------------------------------------
// update/failed_forward.json: pure failed-forward nudge math.
// ---------------------------------------------------------------------------

#[test]
fn failed_forward_math_matches_python_oracle() {
    let doc = load("update/failed_forward.json");

    for case in doc["weight_cases"].as_array().unwrap() {
        let amount_msat = case["amount_msat"].as_i64().unwrap();
        let amount_sats = amount_msat as f64 / 1000.0;
        assert_eq!(
            py_repr(amount_sats),
            case["amount_sats"].as_str().unwrap(),
            "amount_msat={amount_msat}: sats conversion"
        );
        assert_eq!(
            py_repr(failed_forward_nudge_weight(amount_sats)),
            case["expected_weight"].as_str().unwrap(),
            "amount_msat={amount_msat}: nudge weight"
        );
    }

    for case in doc["implied_fee_cases"].as_array().unwrap() {
        let fee = case["current_fee_ppm"].as_i64().unwrap();
        assert_eq!(
            failed_forward_implied_fee(fee),
            case["expected_implied_fee"].as_i64().unwrap(),
            "current_fee_ppm={fee}"
        );
    }

    for case in doc["relevance_cases"].as_array().unwrap() {
        let failcode = case["failcode"].as_i64();
        let failreason = case["failreason"].as_str();
        assert_eq!(
            is_fee_relevant_failure(failcode, failreason),
            case["expected"].as_bool().unwrap(),
            "failcode={failcode:?} failreason={failreason:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Pinned constants new to Task 7.
// ---------------------------------------------------------------------------

#[test]
fn pinned_failcode_constant_matches_python_source() {
    // FEE_RELEVANT_FAILCODES = frozenset({0x1000 | 12}) (py 8501).
    assert_eq!(FEE_RELEVANT_FAILCODES, &[0x1000 | 12]);
    assert_eq!(FEE_RELEVANT_FAILCODES, &[4108]);
}
