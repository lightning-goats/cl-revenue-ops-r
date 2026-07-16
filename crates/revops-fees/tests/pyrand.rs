//! CPython `random.Random` stream parity, pinned by
//! `fixtures/fees/pyrand/sequences.json` (generated from the REAL CPython
//! `random` module by `tools/port/gen_fees_fixtures.py pyrand` in the port
//! worktree, branch `port`).
//!
//! Every expected value is a CPython `repr(float)` string; the assertion
//! is `py_repr(actual) == expected` — bit-for-bit, no epsilon. This suite
//! is also the libm canary of the Global Constraints: `gauss` exercises
//! `ln`/`sqrt`/`cos`/`sin`, so any platform-libm divergence from CPython
//! shows up here first.

use revops_econ::context::CycleContext;
use revops_econ::pyfloat::py_repr;
use revops_econ::types::UnixTime;
use revops_fees::pyrand::PyRandom;
use serde_json::Value;
use std::path::PathBuf;

fn fixture() -> Value {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fees/pyrand/sequences.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

fn seed_entries(fx: &Value) -> &Vec<Value> {
    fx["seeds"].as_array().expect("seeds array")
}

/// The last pinned seed is `CycleContext(seed=0).derive_seed("fee-sample")`
/// — recompute it via `revops_econ::context` and cross-check both the
/// fixture's `fee_sample_seed` field and its presence in the seed list.
#[test]
fn fee_sample_seed_matches_derive_seed() {
    let fx = fixture();
    let ctx = CycleContext::new(
        "c".to_string(),
        UnixTime::new(fx["now"].as_i64().expect("now")).unwrap(),
        0,
        "s".to_string(),
    )
    .unwrap();
    let derived = ctx.derive_seed("fee-sample").unwrap();
    assert_eq!(fx["fee_sample_seed"].as_i64().unwrap(), derived);
    let last = seed_entries(&fx).last().expect("nonempty seeds");
    assert_eq!(last["seed"].as_i64().unwrap(), derived);
}

#[test]
fn covers_planned_seeds() {
    let fx = fixture();
    let seeds: Vec<u64> = seed_entries(&fx)
        .iter()
        .map(|e| e["seed"].as_u64().unwrap())
        .collect();
    assert_eq!(seeds.len(), 5, "five pinned seeds");
    for s in [0u64, 1, 42, (1 << 31) - 1] {
        assert!(seeds.contains(&s), "seed {s} pinned");
    }
}

#[test]
fn random_streams_match_cpython() {
    let fx = fixture();
    for entry in seed_entries(&fx) {
        let seed = entry["seed"].as_u64().unwrap();
        let mut rng = PyRandom::seed_from_u64(seed);
        let expected = entry["random"].as_array().unwrap();
        assert_eq!(expected.len(), 16, "16 random() values per seed");
        for (i, exp) in expected.iter().enumerate() {
            assert_eq!(
                py_repr(rng.random()),
                exp.as_str().unwrap(),
                "seed {seed} random()[{i}]"
            );
        }
    }
}

#[test]
fn gauss_streams_match_cpython() {
    let fx = fixture();
    for entry in seed_entries(&fx) {
        let seed = entry["seed"].as_u64().unwrap();
        let mut rng = PyRandom::seed_from_u64(seed);
        let expected = entry["gauss"].as_array().unwrap();
        assert_eq!(expected.len(), 16, "16 gauss(0,1) values per seed");
        for (i, exp) in expected.iter().enumerate() {
            assert_eq!(
                py_repr(rng.gauss(0.0, 1.0)),
                exp.as_str().unwrap(),
                "seed {seed} gauss(0,1)[{i}]"
            );
        }
    }
}

/// Interleaved `random`/`gauss` call patterns pin the `gauss_next` cached
/// second Box–Muller value across call boundaries: `random()` must NOT
/// clear the cache, a pending cache must be consumed with the CONSUMING
/// call's mu/sigma, and a fresh pair fill must draw exactly two `random()`
/// values from the underlying MT19937 stream.
#[test]
fn interleaved_patterns_match_cpython() {
    let fx = fixture();
    for entry in seed_entries(&fx) {
        let seed = entry["seed"].as_u64().unwrap();
        let patterns = entry["interleaved"].as_array().unwrap();
        assert_eq!(patterns.len(), 3, "three interleave patterns per seed");
        for (p, pat) in patterns.iter().enumerate() {
            let calls = pat["calls"].as_array().unwrap();
            let values = pat["values"].as_array().unwrap();
            assert_eq!(calls.len(), values.len());
            let mut rng = PyRandom::seed_from_u64(seed);
            for (i, (call, exp)) in calls.iter().zip(values).enumerate() {
                let got = match call["op"].as_str().unwrap() {
                    "random" => rng.random(),
                    "gauss" => {
                        let mu: f64 = call["mu"].as_str().unwrap().parse().unwrap();
                        let sigma: f64 = call["sigma"].as_str().unwrap().parse().unwrap();
                        rng.gauss(mu, sigma)
                    }
                    other => panic!("unknown op {other}"),
                };
                assert_eq!(
                    py_repr(got),
                    exp.as_str().unwrap(),
                    "seed {seed} pattern {p} call {i}"
                );
            }
        }
    }
}
