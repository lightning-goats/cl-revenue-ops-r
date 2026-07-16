//! 3x3 matrix kernel parity, pinned by `fixtures/fees/mat3/*.json`
//! (generated from the REAL `GaussianThompsonState._mat3_det/_mat3_invert/
//! _mat3_vec_mul/_cholesky3` static methods — `fee_controller.py:468-528`
//! — by `tools/port/gen_fees_fixtures.py mat3` in the port worktree).
//!
//! Inputs and expected values are CPython `repr(float)` strings; inputs
//! parse exactly (shortest-round-trip), outputs compare via
//! `py_repr(actual) == expected`. `null` expectations pin the singularity /
//! non-PD branches, including BOTH sides of `_mat3_invert`'s RELATIVE
//! `1e-10 * max(1, max_elem^3)` tolerance.

use revops_econ::pyfloat::py_repr;
use revops_fees::mat3::{cholesky3, det3, invert3, matvec3, M3, V3};
use serde_json::Value;
use std::path::PathBuf;

fn fixture(name: &str) -> Vec<Value> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../../fixtures/fees/mat3/{name}.json"));
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let v: Value = serde_json::from_str(&raw).expect("valid JSON");
    v["cases"].as_array().expect("cases array").clone()
}

fn parse_f(v: &Value) -> f64 {
    v.as_str().expect("repr string").parse().expect("parse f64")
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

fn parse_v3(v: &Value) -> V3 {
    let xs = v.as_array().expect("3 elems");
    [parse_f(&xs[0]), parse_f(&xs[1]), parse_f(&xs[2])]
}

fn repr_m3(m: &M3) -> Vec<Vec<String>> {
    m.iter()
        .map(|r| r.iter().map(|&x| py_repr(x)).collect())
        .collect()
}

fn expected_m3(v: &Value) -> Option<Vec<Vec<String>>> {
    if v.is_null() {
        return None;
    }
    Some(
        v.as_array()
            .unwrap()
            .iter()
            .map(|r| {
                r.as_array()
                    .unwrap()
                    .iter()
                    .map(|x| x.as_str().unwrap().to_string())
                    .collect()
            })
            .collect(),
    )
}

#[test]
fn det_matches_python() {
    let cases = fixture("det");
    assert!(cases.len() >= 20, "det suite has >= 20 vectors");
    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let m = parse_m3(&case["m"]);
        assert_eq!(
            py_repr(det3(&m)),
            case["expected"].as_str().unwrap(),
            "det3 case {name}"
        );
    }
}

#[test]
fn invert_matches_python() {
    let cases = fixture("invert");
    assert!(cases.len() >= 20, "invert suite has >= 20 vectors");
    let mut nulls = 0;
    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let m = parse_m3(&case["m"]);
        let expected = expected_m3(&case["expected"]);
        if expected.is_none() {
            nulls += 1;
        }
        assert_eq!(
            invert3(&m).as_ref().map(repr_m3),
            expected,
            "invert3 case {name}"
        );
    }
    // Both sides of the relative tolerance must be pinned: at least the
    // exact-singular, zero, below-rel-tol and below-abs-floor cases.
    assert!(nulls >= 4, "invert suite pins >= 4 singular branches");
}

#[test]
fn invert_relative_tolerance_branch_pins_present() {
    let cases = fixture("invert");
    let get = |n: &str| {
        cases
            .iter()
            .find(|c| c["name"] == n)
            .unwrap_or_else(|| panic!("case {n} present"))
            .clone()
    };
    // Below the RELATIVE tolerance (det 1e-5 > absolute 1e-10 but the
    // max_elem^3 scaling rejects it) => null.
    assert!(get("near_singular_below_rel_tol")["expected"].is_null());
    assert!(!get("near_singular_above_rel_tol")["expected"].is_null());
    // At the 1.0 floor of max(1.0, max_elem^3) the tolerance is absolute.
    assert!(get("small_below_abs_tol")["expected"].is_null());
    assert!(!get("small_above_abs_tol")["expected"].is_null());
}

#[test]
fn matvec_matches_python() {
    let cases = fixture("matvec");
    assert!(cases.len() >= 20, "matvec suite has >= 20 vectors");
    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let m = parse_m3(&case["m"]);
        let v = parse_v3(&case["v"]);
        let got: Vec<String> = matvec3(&m, &v).iter().map(|&x| py_repr(x)).collect();
        let expected: Vec<String> = case["expected"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_str().unwrap().to_string())
            .collect();
        assert_eq!(got, expected, "matvec3 case {name}");
    }
}

#[test]
fn cholesky_matches_python() {
    let cases = fixture("cholesky");
    assert!(cases.len() >= 20, "cholesky suite has >= 20 vectors");
    let mut nulls = 0;
    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let m = parse_m3(&case["m"]);
        let expected = expected_m3(&case["expected"]);
        if expected.is_none() {
            nulls += 1;
        }
        assert_eq!(
            cholesky3(&m).as_ref().map(repr_m3),
            expected,
            "cholesky3 case {name}"
        );
    }
    // Non-PD rejections must be pinned (negative pivot + late pivot).
    assert!(nulls >= 2, "cholesky suite pins >= 2 non-PD branches");
    let non_pd = cases.iter().find(|c| c["name"] == "non_pd_diag").unwrap();
    assert!(non_pd["expected"].is_null(), "non-PD matrix -> null");
}

/// The DTS default prior precision `0.01 * I` (fee_controller.py:405-407)
/// must be present in every suite — it is the matrix every fresh channel
/// posterior starts from.
#[test]
fn dts_default_prior_case_present() {
    for suite in ["det", "invert", "cholesky"] {
        assert!(
            fixture(suite)
                .iter()
                .any(|c| c["name"] == "dts_default_prior"),
            "{suite} covers the DTS default prior"
        );
    }
}

/// Realistic post-fit precision matrices (dumped from the REAL Python
/// `_recompute_posterior_core` over 3 seeded observation sets) must be
/// present — these are the matrices `invert3`/`cholesky3` see in
/// production.
#[test]
fn posterior_precision_cases_present() {
    for suite in ["det", "invert", "cholesky"] {
        let cases = fixture(suite);
        for seed in 1..=3 {
            let name = format!("posterior_precision_seed{seed}");
            assert!(
                cases.iter().any(|c| c["name"] == name.as_str()),
                "{suite} covers {name}"
            );
        }
    }
}
