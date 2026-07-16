//! Corpus-replay + determinism tests for `revops_econ::arbiter`.
//!
//! Envelopes are built with `make_intent`, reconstructing the generator's
//! `_env()` defaults (`tools/conformance/generate_scenarios.py` in
//! `cl_revenue_ops-port`) field-for-field, so the derived `intent_id`/
//! `idempotency_key` land on the same values pinned in each scenario's
//! `case.json` `expected` block — this is checked directly, not assumed.
//!
//! Scenarios replayed: `tests/conformance/scenarios/{18,20,21,31,35,38}-*/
//! case.json` in `cl_revenue_ops-port`.

use revops_econ::arbiter::{arbitrate, ActiveIntentRegistry, ArbitrationResult};
use revops_econ::intents::{make_intent, Explanation, IntentEnvelope, IntentFields};
use revops_econ::types::{Micro, Msat, SignedMsat, UnixTime};

const NOW: i64 = 1_752_400_000;

struct Args {
    intent_type: &'static str,
    target: String,
    amount_sats: i64,
    benefit: i64,
    max_cost_sats: i64,
    capital_sats: Option<i64>,
    confidence: i64,
    priority: i32,
    bucket: &'static str,
    policy: &'static str,
    created: i64,
    expires: Option<i64>,
}

impl Default for Args {
    fn default() -> Self {
        Args {
            intent_type: "REBALANCE",
            target: "111x222x0".to_string(),
            amount_sats: 400_000,
            benefit: 0,
            max_cost_sats: 3_000,
            capital_sats: None,
            confidence: 0,
            priority: 50,
            bucket: "rebalance",
            policy: "conformance",
            created: NOW,
            expires: None,
        }
    }
}

/// Mirrors the generator's `_env()` helper exactly, including its "amount 0
/// is falsy -> no amount_msat" and "capital defaults to amount" quirks:
/// ```python
/// def _env(intent_type="REBALANCE", target="111x222x0", amount=400_000,
///          benefit=0, max_cost=3_000, capital=None, confidence=0,
///          priority=50, bucket="rebalance", snapshot_id="snap-1",
///          created=NOW, expires=None, reason_codes=(), policy="conformance"):
///     return make_intent(
///         ..., amount_msat=Msat(amount * 1000) if amount else None, ...,
///         capital_committed_msat=Msat((capital if capital is not None
///                                      else amount or 0) * 1000), ...)
/// ```
fn env(args: Args) -> IntentEnvelope {
    let amount_msat = if args.amount_sats != 0 {
        Some(Msat::new(args.amount_sats * 1000).unwrap())
    } else {
        None
    };
    let capital_sats = args.capital_sats.unwrap_or(if args.amount_sats != 0 {
        args.amount_sats
    } else {
        0
    });
    let expires = args.expires.unwrap_or(args.created + 600);
    make_intent(IntentFields {
        intent_type: args.intent_type.to_string(),
        snapshot_id: "snap-1".to_string(),
        created_at: UnixTime::new(args.created).unwrap(),
        expires_at: UnixTime::new(expires).unwrap(),
        target: args.target,
        amount_msat,
        expected_benefit_msat: SignedMsat(args.benefit),
        max_cost_msat: Msat::new(args.max_cost_sats * 1000).unwrap(),
        capital_committed_msat: Msat::new(capital_sats * 1000).unwrap(),
        confidence_micro: Micro::new(args.confidence).unwrap(),
        reason_codes: vec![],
        explanation: Explanation {
            kind: "conformance".to_string(),
            components: vec![("case".to_string(), serde_json::json!(1))],
        },
        preconditions: vec![],
        priority: args.priority,
        budget_bucket: args.bucket.to_string(),
        origin_policy: args.policy.to_string(),
        reversible: false,
    })
    .unwrap()
}

fn ordered_ids(result: &ArbitrationResult) -> Vec<String> {
    result
        .ordered
        .iter()
        .map(|e| e.intent_id.as_str().to_string())
        .collect()
}

fn rejected_tuples(result: &ArbitrationResult) -> Vec<(String, String, String)> {
    result
        .rejected
        .iter()
        .map(|(env, code, detail)| {
            (
                env.intent_id.as_str().to_string(),
                code.clone(),
                detail.clone(),
            )
        })
        .collect()
}

// --- scenario 18: 18-conflicting-close-rebalance ---

#[test]
fn scenario_18_conflicting_close_and_rebalance() {
    let close = env(Args {
        intent_type: "CLOSE_CHANNEL",
        target: "111x222x0".to_string(),
        amount_sats: 0,
        max_cost_sats: 0,
        bucket: "channel_open",
        priority: 60,
        ..Default::default()
    });
    let reb = env(Args {
        intent_type: "REBALANCE",
        target: "111x222x0".to_string(),
        ..Default::default()
    });
    assert_eq!(close.intent_id.as_str(), "int-9c2fdcd788a9c1f0");
    assert_eq!(reb.intent_id.as_str(), "int-1119af2f103eb344");

    let result = arbitrate(&[close, reb], NOW, false);

    assert_eq!(ordered_ids(&result), vec!["int-9c2fdcd788a9c1f0"]);
    assert_eq!(
        rejected_tuples(&result),
        vec![(
            "int-1119af2f103eb344".to_string(),
            "CONFLICT_CLOSE_REBALANCE".to_string(),
            "target 111x222x0 has a close intent".to_string(),
        )]
    );
}

// --- scenario 20: 20-open-vs-lnplus ---

#[test]
fn scenario_20_open_vs_lnplus_conflict() {
    let peer = "02".to_string() + &"b".repeat(64);
    let lnplus = env(Args {
        intent_type: "OPEN_CHANNEL",
        target: peer.clone(),
        amount_sats: 2_000_000,
        priority: 80,
        bucket: "channel_open",
        policy: "lnplus_lifecycle_governed",
        ..Default::default()
    });
    let planner = env(Args {
        intent_type: "OPEN_CHANNEL",
        target: peer,
        amount_sats: 1_000_000,
        priority: 50,
        bucket: "channel_open",
        policy: "planner",
        ..Default::default()
    });
    assert_eq!(lnplus.intent_id.as_str(), "int-3d0c93c3b4d98c38");
    assert_eq!(planner.intent_id.as_str(), "int-7a1a535688437c94");

    // Generator calls arbitrate([planner, lnplus], ...) — input order per case.json.
    let result = arbitrate(&[planner, lnplus], NOW, true);

    assert_eq!(ordered_ids(&result), vec!["int-3d0c93c3b4d98c38"]);
    assert_eq!(
        rejected_tuples(&result),
        vec![(
            "int-7a1a535688437c94".to_string(),
            "CONFLICT_DUPLICATE_OPEN".to_string(),
            format!("target 02{} already has an open intent", "b".repeat(64)),
        )]
    );
}

// --- scenario 21: 21-circular-vs-boltz-structural ---

#[test]
fn scenario_21_circular_rebalance_vs_boltz_structural() {
    let swap = env(Args {
        intent_type: "SWAP_OUT",
        target: "111x222x0".to_string(),
        amount_sats: 250_000,
        bucket: "rebalance",
        policy: "boltz",
        ..Default::default()
    });
    let reb = env(Args {
        intent_type: "REBALANCE",
        target: "111x222x0".to_string(),
        ..Default::default()
    });
    assert_eq!(swap.intent_id.as_str(), "int-02f9475d26486caf");
    assert_eq!(reb.intent_id.as_str(), "int-1119af2f103eb344");

    let result = arbitrate(&[reb, swap], NOW, true);

    assert_eq!(ordered_ids(&result), vec!["int-02f9475d26486caf"]);
    assert_eq!(
        rejected_tuples(&result),
        vec![(
            "int-1119af2f103eb344".to_string(),
            "CONFLICT_REBALANCE_SWAP".to_string(),
            "target 111x222x0 has a structural swap intent".to_string(),
        )]
    );
}

// --- scenario 31: 31-duplicate-idempotency-key ---

#[test]
fn scenario_31_duplicate_idempotency_key() {
    let a = env(Args {
        target: "111x222x0".to_string(),
        ..Default::default()
    });
    let b = env(Args {
        target: "111x222x0".to_string(),
        ..Default::default()
    });
    assert_eq!(a.idempotency_key, b.idempotency_key);
    assert_eq!(a.intent_id.as_str(), "int-1119af2f103eb344");

    let result = arbitrate(&[a, b], NOW, false);

    assert_eq!(ordered_ids(&result), vec!["int-1119af2f103eb344"]);
    assert_eq!(
        rejected_tuples(&result),
        vec![(
            "int-1119af2f103eb344".to_string(),
            "INTENT_SUPERSEDED".to_string(),
            "duplicate idempotency key".to_string(),
        )]
    );
    assert_eq!(
        result.superseded.get("int-1119af2f103eb344"),
        Some(&"int-1119af2f103eb344".to_string())
    );
}

// --- scenario 35: 35-stable-ordering-tiebreak ---

fn scenario_35_envs() -> Vec<IntentEnvelope> {
    vec![
        env(Args {
            target: "300x1x0".to_string(),
            priority: 50,
            benefit: 5_000,
            ..Default::default()
        }),
        env(Args {
            target: "100x1x0".to_string(),
            priority: 50,
            benefit: 5_000,
            ..Default::default()
        }),
        env(Args {
            target: "200x1x0".to_string(),
            priority: 80,
            benefit: 0,
            ..Default::default()
        }),
        env(Args {
            target: "400x1x0".to_string(),
            priority: 50,
            benefit: 9_000,
            ..Default::default()
        }),
    ]
}

const SCENARIO_35_EXPECTED_ORDER: [&str; 4] = [
    "int-6ca7481bc9571b1b",
    "int-7c4fe00860f83371",
    "int-f39fe2e07353af1f",
    "int-7da60549413f85a1",
];

#[test]
fn scenario_35_stable_ordering_tie_break() {
    let envs = scenario_35_envs();
    // Sanity: the ids the corpus pins really are the ids we derive.
    let mut ids: Vec<&str> = envs.iter().map(|e| e.intent_id.as_str()).collect();
    ids.sort();
    let mut expected_sorted = SCENARIO_35_EXPECTED_ORDER;
    expected_sorted.sort();
    assert_eq!(ids, expected_sorted);

    let result = arbitrate(&envs, NOW, false);

    assert_eq!(
        ordered_ids(&result),
        SCENARIO_35_EXPECTED_ORDER
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    );
    assert!(result.rejected.is_empty());
}

/// Order-insensitivity: same intents (any input order) + same `now` =>
/// byte-identical `ArbitrationResult` (J3). Uses a fixed, deliberately
/// scrambled permutation rather than a random shuffle — no `rand` dependency
/// is permitted in this crate (see the phase plan's Global Constraints).
#[test]
fn arbitrate_is_order_insensitive() {
    let forward = scenario_35_envs();
    let raw = scenario_35_envs();
    // Fixed, deliberately scrambled permutation: [2, 0, 3, 1].
    let shuffled = vec![
        raw[2].clone(),
        raw[0].clone(),
        raw[3].clone(),
        raw[1].clone(),
    ];
    // Just to be sure this really is a different order from `forward`.
    let forward_ids: Vec<&str> = forward.iter().map(|e| e.intent_id.as_str()).collect();
    let shuffled_ids: Vec<&str> = shuffled.iter().map(|e| e.intent_id.as_str()).collect();
    assert_ne!(forward_ids, shuffled_ids);

    let result_forward = arbitrate(&forward, NOW, false);
    let result_shuffled = arbitrate(&shuffled, NOW, false);

    assert_eq!(result_forward, result_shuffled);
    assert_eq!(
        ordered_ids(&result_forward),
        SCENARIO_35_EXPECTED_ORDER
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    );
}

// --- scenario 38: 38-partial-batch-completion ---

#[test]
fn scenario_38_partial_batch_completion() {
    let close = env(Args {
        intent_type: "CLOSE_CHANNEL",
        target: "500x1x0".to_string(),
        amount_sats: 0,
        max_cost_sats: 0,
        bucket: "channel_open",
        priority: 60,
        ..Default::default()
    });
    let ok = env(Args {
        target: "600x1x0".to_string(),
        ..Default::default()
    });
    let conflicted = env(Args {
        target: "500x1x0".to_string(),
        ..Default::default()
    });
    let dup = env(Args {
        target: "600x1x0".to_string(),
        ..Default::default()
    });

    assert_eq!(close.intent_id.as_str(), "int-3ccd42809618aef8");
    assert_eq!(ok.intent_id.as_str(), "int-8c5dd83990a13524");
    assert_eq!(conflicted.intent_id.as_str(), "int-a6b3b9ad3da78e42");
    assert_eq!(dup.intent_id.as_str(), ok.intent_id.as_str());

    let result = arbitrate(&[close, ok, conflicted, dup], NOW, false);

    assert_eq!(
        ordered_ids(&result),
        vec![
            "int-3ccd42809618aef8".to_string(),
            "int-8c5dd83990a13524".to_string(),
        ]
    );
    assert_eq!(
        rejected_tuples(&result),
        vec![
            (
                "int-8c5dd83990a13524".to_string(),
                "INTENT_SUPERSEDED".to_string(),
                "duplicate idempotency key".to_string(),
            ),
            (
                "int-a6b3b9ad3da78e42".to_string(),
                "CONFLICT_CLOSE_REBALANCE".to_string(),
                "target 500x1x0 has a close intent".to_string(),
            ),
        ]
    );
}

/// Same batch as scenario 38, shuffled input order, must land on the
/// identical result (order-insensitivity, independent confirmation using a
/// batch with actual rejections rather than an all-survivors one).
#[test]
fn scenario_38_is_order_insensitive() {
    let build = || {
        let close = env(Args {
            intent_type: "CLOSE_CHANNEL",
            target: "500x1x0".to_string(),
            amount_sats: 0,
            max_cost_sats: 0,
            bucket: "channel_open",
            priority: 60,
            ..Default::default()
        });
        let ok = env(Args {
            target: "600x1x0".to_string(),
            ..Default::default()
        });
        let conflicted = env(Args {
            target: "500x1x0".to_string(),
            ..Default::default()
        });
        let dup = env(Args {
            target: "600x1x0".to_string(),
            ..Default::default()
        });
        (close, ok, conflicted, dup)
    };

    let (close, ok, conflicted, dup) = build();
    let forward = vec![close, ok, conflicted, dup];

    let (close2, ok2, conflicted2, dup2) = build();
    let shuffled = vec![dup2, conflicted2, ok2, close2];

    let result_forward = arbitrate(&forward, NOW, false);
    let result_shuffled = arbitrate(&shuffled, NOW, false);
    assert_eq!(result_forward, result_shuffled);
}

// --- ActiveIntentRegistry: live-vs-batch divergence (registered here too,
// alongside the corpus replays, since s18/s21's fixtures are convenient
// shared shapes) ---

#[test]
fn registry_live_close_then_rebalance_matches_batch_rejection() {
    let registry = ActiveIntentRegistry::new(None);
    let close = env(Args {
        intent_type: "CLOSE_CHANNEL",
        target: "111x222x0".to_string(),
        amount_sats: 0,
        max_cost_sats: 0,
        bucket: "channel_open",
        priority: 60,
        ..Default::default()
    });
    let reb = env(Args {
        intent_type: "REBALANCE",
        target: "111x222x0".to_string(),
        ..Default::default()
    });
    assert_eq!(registry.check_and_register(&close, NOW), None);
    assert_eq!(
        registry.check_and_register(&reb, NOW),
        Some("CONFLICT_CLOSE_REBALANCE")
    );
}

#[test]
fn registry_live_rebalance_then_close_diverges_from_batch() {
    // Batch (scenario 18 shape) always rejects the REBALANCE once any
    // CLOSE_CHANNEL exists among the candidates, regardless of original
    // input order (it sorts everything first). Live only checks from the
    // incoming intent's point of view, so the reverse arrival order is NOT
    // blocked: a CLOSE_CHANNEL registering after an active REBALANCE on the
    // same target succeeds. This is the deliberate live/batch divergence.
    let registry = ActiveIntentRegistry::new(None);
    let reb = env(Args {
        intent_type: "REBALANCE",
        target: "111x222x0".to_string(),
        ..Default::default()
    });
    let close = env(Args {
        intent_type: "CLOSE_CHANNEL",
        target: "111x222x0".to_string(),
        amount_sats: 0,
        max_cost_sats: 0,
        bucket: "channel_open",
        priority: 60,
        ..Default::default()
    });
    assert_eq!(registry.check_and_register(&reb, NOW), None);
    assert_eq!(registry.check_and_register(&close, NOW), None);
    assert_eq!(registry.active_count(NOW), 2);
}
