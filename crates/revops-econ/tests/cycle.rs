//! Deterministic cycle core tests, transcribed from
//! `tests/test_econ_cycle.py`'s `TestDeterminism` / `TestBatchArbitration`
//! classes (`TestShadowRunner` is out of scope — `run_shadow_cycle`'s live
//! collector is deferred to Phase 2b wiring per the Task 8 brief).

use revops_econ::context::CycleContext;
use revops_econ::cycle::{plan_cycle, RebalancePair};
use revops_econ::types::UnixTime;

const NOW: i64 = 1_752_400_000;

fn ctx(seed: i64) -> CycleContext {
    CycleContext::new(
        "econ-cycle-1".to_string(),
        UnixTime::new(NOW).unwrap(),
        seed,
        "econ-cycle-1".to_string(),
    )
    .unwrap()
}

struct PairArgs {
    src: &'static str,
    dst: &'static str,
    amount: i64,
    budget: i64,
    score: f64,
}

impl Default for PairArgs {
    fn default() -> Self {
        PairArgs {
            src: "100x1x0",
            dst: "200x1x0",
            amount: 500_000,
            budget: 100,
            score: 0.298,
        }
    }
}

fn pair(args: PairArgs) -> RebalancePair {
    RebalancePair {
        source_channel_id: args.src.to_string(),
        dest_channel_id: args.dst.to_string(),
        amount_sats: args.amount,
        pair_budget_sats: args.budget,
        score: args.score,
        score_decomposition: None,
    }
}

// --- TestDeterminism ---

#[test]
fn byte_identical_across_runs() {
    let pairs = vec![
        pair(PairArgs::default()),
        pair(PairArgs {
            src: "300x1x0",
            dst: "400x1x0",
            amount: 250_000,
            ..Default::default()
        }),
    ];
    let a = plan_cycle(&pairs, &ctx(42), 2).unwrap();
    let b = plan_cycle(&pairs, &ctx(42), 2).unwrap();
    assert_eq!(a.canonical().unwrap(), b.canonical().unwrap());
}

#[test]
fn input_order_never_matters() {
    let pairs = vec![
        pair(PairArgs::default()),
        pair(PairArgs {
            src: "300x1x0",
            dst: "400x1x0",
            ..Default::default()
        }),
        pair(PairArgs {
            src: "500x1x0",
            dst: "600x1x0",
            budget: 200,
            ..Default::default()
        }),
    ];
    let baseline = plan_cycle(&pairs, &ctx(42), 3)
        .unwrap()
        .canonical()
        .unwrap();

    // Deterministic "shuffles": every rotation/reversal of the 3-element
    // input, standing in for `random.Random(seed).shuffle` in the Python
    // original — what matters is covering multiple non-identity orderings,
    // not reproducing Python's PRNG bit-for-bit.
    let shuffles: Vec<Vec<RebalancePair>> = vec![
        vec![pairs[2].clone(), pairs[0].clone(), pairs[1].clone()],
        vec![pairs[1].clone(), pairs[2].clone(), pairs[0].clone()],
        vec![pairs[2].clone(), pairs[1].clone(), pairs[0].clone()],
        vec![pairs[0].clone(), pairs[2].clone(), pairs[1].clone()],
    ];
    for shuffled in shuffles {
        let result = plan_cycle(&shuffled, &ctx(42), 3)
            .unwrap()
            .canonical()
            .unwrap();
        assert_eq!(result, baseline);
    }
}

#[test]
fn different_context_different_ids_same_shape() {
    let pairs = vec![pair(PairArgs::default())];
    let a = plan_cycle(&pairs, &ctx(1), 1).unwrap();
    let b = plan_cycle(&pairs, &ctx(2), 1).unwrap();
    assert_ne!(a.to_wire()["seed"], b.to_wire()["seed"]);
    // Intent identity derives from snapshot/target/amount — the seed does
    // not perturb intent ids (no randomness consumed in v0).
    assert_eq!(
        a.to_wire()["ordered"][0]["intent_id"],
        b.to_wire()["ordered"][0]["intent_id"]
    );
}

// --- TestBatchArbitration ---

#[test]
fn duplicate_pairs_superseded_in_batch() {
    let pairs = vec![pair(PairArgs::default()), pair(PairArgs::default())];
    let result = plan_cycle(&pairs, &ctx(42), 2).unwrap();
    assert_eq!(result.arbitration.ordered.len(), 1);
    assert_eq!(result.arbitration.rejected[0].1, "INTENT_SUPERSEDED");
}

#[test]
fn ordering_follows_j3_ladder() {
    // Equal priority/benefit/confidence/capital -> target then id.
    let pairs = vec![
        pair(PairArgs {
            dst: "900x1x0",
            ..Default::default()
        }),
        pair(PairArgs {
            dst: "100x1x0",
            ..Default::default()
        }),
    ];
    let result = plan_cycle(&pairs, &ctx(42), 2).unwrap();
    let targets: Vec<&str> = result
        .arbitration
        .ordered
        .iter()
        .map(|e| e.target.as_str())
        .collect();
    let mut sorted_targets = targets.clone();
    sorted_targets.sort_unstable();
    assert_eq!(targets, sorted_targets);
}

#[test]
fn wire_result_shape() {
    let pairs = vec![pair(PairArgs::default())];
    let result = plan_cycle(&pairs, &ctx(42), 1).unwrap();
    let wire = result.to_wire();
    assert_eq!(wire["schema_name"], "econ_cycle_result");
    assert_eq!(wire["intents_proposed"], 1);
    assert_eq!(wire["ordered"][0]["origin_policy"], "econ_cycle_shadow");
}

// --- seed pin (Task 8 brief: CycleContext{seed:0}.derive_seed("econ-cycle")
// equals Python's value for cycle id "econ-cycle-{now}-{seq}") ---

#[test]
fn derive_seed_zero_econ_cycle_matches_python_golden_value() {
    // Same golden pair already pinned in `context.rs`'s own tests
    // (`derive_seed_matches_python_golden_pairs`), re-asserted here against
    // a cycle_id shaped like the real `run_shadow_cycle` naming scheme
    // (`f"econ-cycle-{int(now)}-{int(cycle_seq)}"`) to pin the exact
    // construction Task 8 cares about.
    let base = CycleContext::new(
        format!("econ-cycle-{NOW}-1"),
        UnixTime::new(NOW).unwrap(),
        0,
        format!("econ-cycle-{NOW}-1"),
    )
    .unwrap();
    assert_eq!(
        base.derive_seed("econ-cycle").unwrap(),
        1_161_200_426_328_304_218
    );
}

// --- explanation / float-in-explanation shape ---

#[test]
fn explanation_carries_rounded_score_and_survives_canonical() {
    let pairs = vec![pair(PairArgs {
        score: 1.0 / 3.0,
        ..Default::default()
    })];
    let result = plan_cycle(&pairs, &ctx(42), 1).unwrap();
    let wire = result.to_wire();
    let components = wire["ordered"][0]["explanation"]["components"]
        .as_array()
        .unwrap();
    let score_component = components
        .iter()
        .find(|c| c[0] == "score")
        .expect("score component present");
    assert_eq!(score_component[1].as_f64().unwrap(), 0.333333);

    // canonical() must not error (the local writer handles the float leaf
    // instead of revops_core::canonical_json's fail-closed rejection) and
    // must render the score via py_repr, not Rust's native f64 Display.
    let canon = result.canonical().unwrap();
    assert!(canon.contains("0.333333"));
}

#[test]
fn ev_disabled_by_default_keeps_zeros() {
    let pairs = vec![pair(PairArgs::default())];
    let result = plan_cycle(&pairs, &ctx(42), 1).unwrap();
    let env = &result.arbitration.ordered[0];
    assert_eq!(env.expected_benefit_msat.0, 0);
    assert_eq!(env.confidence_micro.value(), 0);
}

#[test]
fn amount_and_capital_are_amount_sats_times_1000() {
    let pairs = vec![pair(PairArgs {
        amount: 500_000,
        budget: 100,
        ..Default::default()
    })];
    let result = plan_cycle(&pairs, &ctx(42), 1).unwrap();
    let env = &result.arbitration.ordered[0];
    assert_eq!(env.amount_msat.unwrap().value(), 500_000_000);
    assert_eq!(env.capital_committed_msat.value(), 500_000_000);
    assert_eq!(env.max_cost_msat.value(), 100_000);
    assert_eq!(env.priority, 50);
    assert_eq!(env.budget_bucket, "rebalance");
    assert_eq!(env.origin_policy, "econ_cycle_shadow");
    assert!(!env.reversible);
    assert_eq!(env.expires_at.value() - env.created_at.value(), 600);
}
