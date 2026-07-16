//! Batch arbiter + live `ActiveIntentRegistry` (port of `modules/econ_arbiter.py`).
//!
//! Two DELIBERATELY different semantics live here, mirroring the Python
//! module exactly — never unify them:
//!
//! - [`arbitrate`]: pure, deterministic BATCH resolution over a whole set of
//!   candidate intents. Same intents + same `now` => byte-identical result
//!   regardless of input order (sorted first by the J3 tie-break ladder,
//!   then six ordered rules run over the sorted list).
//! - [`ActiveIntentRegistry`]: LIVE, stateful, order-of-ARRIVAL semantics at
//!   the governor boundary. It only ever looks at "does the incoming intent
//!   conflict with something already active" — it never retroactively
//!   rejects something already registered, and (legacy rule) only checks
//!   the REBALANCE side of the close-vs-rebalance conflict. This means live
//!   and batch can genuinely disagree on the same set of intents depending
//!   on arrival order; that divergence is intentional (see the registry
//!   tests below and in `tests/arbiter.rs`).
//!
//! Rule order (batch, frozen): stale -> duplicate idempotency keys
//! (best-sorted wins) -> SET_FEE/SET_HTLC_MAX per (type, target) ->
//! close-vs-rebalance -> \[extended\] dup-open -> rebalance-vs-SWAP_OUT
//! (swap outranks).
//!
//! Detail strings are wire-frozen: "expired before arbitration",
//! "duplicate idempotency key", "conflicting {intent_type} on {target}",
//! "target {t} has a close intent", "target {t} already has an open
//! intent", "target {t} has a structural swap intent".

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;

use crate::intents::IntentEnvelope;
use crate::reason::Code;

/// Spec precedence classes (lower = higher precedence), per
/// `DEFAULT_INTENT_PRECEDENCE` in the Python source: `contractual_obligation`
/// 0, `funds_safety` 1, `operator_constraint` 2, `capital_preservation` 3,
/// `revenue_protection` 4, `liquidity_maintenance` 5, `growth` 6. Only the
/// classes actually reachable by the current `INTENT_TYPES` vocabulary are
/// assigned below; unknown/future intent types default to `growth` (6),
/// matching Python's `dict.get(..., "growth")` fallback.
pub fn precedence_class(env: &IntentEnvelope) -> i64 {
    match env.intent_type.as_str() {
        "MAINTAIN_ONCHAIN_RESERVE" => 1,             // funds_safety
        "CLOSE_CHANNEL" => 3,                        // capital_preservation
        "SET_FEE" | "SET_HTLC_MAX" => 4,             // revenue_protection
        "REBALANCE" | "SWAP_IN" | "SWAP_OUT" => 5,   // liquidity_maintenance
        "OPEN_CHANNEL" | "JOIN_LIQUIDITY_SWAP" => 6, // growth
        _ => 6,                                      // growth (Python dict.get default)
    }
}

/// J3 tie-break ladder, ascending: `(precedence, -priority, -expected_benefit,
/// -confidence, capital_committed, target, intent_id)`. Negated terms are
/// widened to `i128` before negation so no boundary value (e.g.
/// `expected_benefit_msat == i64::MIN`) can overflow — the *value* of the
/// key, not its representable range, is what must match Python's tuple
/// comparison.
type SortKey = (i64, i32, i128, i64, i64, String, String);

fn sort_key(env: &IntentEnvelope) -> SortKey {
    (
        precedence_class(env),
        -env.priority,
        -(env.expected_benefit_msat.0 as i128),
        -env.confidence_micro.value(),
        env.capital_committed_msat.value(),
        env.target.clone(),
        env.intent_id.as_str().to_string(),
    )
}

/// Result of a single batch [`arbitrate`] call.
#[derive(Debug, Clone, PartialEq)]
pub struct ArbitrationResult {
    /// Surviving intents, in final decision order.
    pub ordered: Vec<IntentEnvelope>,
    /// Rejected intents with their `(reason_code, detail)`.
    pub rejected: Vec<(IntentEnvelope, String, String)>,
    /// Rejected `intent_id` -> the superseding intent's `intent_id`, for the
    /// rules that have a well-defined "winner" (duplicate idempotency keys,
    /// contradictory policy changes, and — extended only — duplicate opens).
    pub superseded: BTreeMap<String, String>,
}

/// Batch arbitration. Pure and deterministic: sorts a private copy of
/// `intents` by [`sort_key`] first, so the result never depends on input
/// order, then applies the six frozen rules in order.
pub fn arbitrate(intents: &[IntentEnvelope], now: i64, extended_rules: bool) -> ArbitrationResult {
    let mut candidates: Vec<IntentEnvelope> = intents.to_vec();
    candidates.sort_by_key(sort_key);

    let mut rejected: Vec<(IntentEnvelope, String, String)> = Vec::new();
    let mut superseded: BTreeMap<String, String> = BTreeMap::new();

    // 1. Staleness (inclusive boundary: now >= expires_at, matches `is_expired`).
    let mut fresh = Vec::with_capacity(candidates.len());
    for env in candidates {
        if now >= env.expires_at.value() {
            rejected.push((
                env,
                Code::IntentStale.as_str().to_string(),
                "expired before arbitration".to_string(),
            ));
        } else {
            fresh.push(env);
        }
    }

    // 2. Duplicate idempotency keys — first (best-sorted) wins.
    let mut seen_keys: HashMap<String, IntentEnvelope> = HashMap::new();
    let mut deduped = Vec::with_capacity(fresh.len());
    for env in fresh {
        if let Some(winner) = seen_keys.get(&env.idempotency_key) {
            superseded.insert(
                env.intent_id.as_str().to_string(),
                winner.intent_id.as_str().to_string(),
            );
            rejected.push((
                env,
                Code::IntentSuperseded.as_str().to_string(),
                "duplicate idempotency key".to_string(),
            ));
            continue;
        }
        seen_keys.insert(env.idempotency_key.clone(), env.clone());
        deduped.push(env);
    }

    // 3. Contradictory SET_FEE / SET_HTLC_MAX on one target — best-sorted wins.
    let mut policy_winner: HashMap<(String, String), IntentEnvelope> = HashMap::new();
    let mut filtered = Vec::with_capacity(deduped.len());
    for env in deduped {
        if env.intent_type == "SET_FEE" || env.intent_type == "SET_HTLC_MAX" {
            let slot = (env.intent_type.clone(), env.target.clone());
            if let Some(winner) = policy_winner.get(&slot) {
                let detail = format!("conflicting {} on {}", env.intent_type, env.target);
                superseded.insert(
                    env.intent_id.as_str().to_string(),
                    winner.intent_id.as_str().to_string(),
                );
                rejected.push((env, Code::IntentSuperseded.as_str().to_string(), detail));
                continue;
            }
            policy_winner.insert(slot, env.clone());
        }
        filtered.push(env);
    }

    // 4. Close-vs-rebalance: a channel being closed must not be rebalanced into.
    let closing_targets: HashSet<String> = filtered
        .iter()
        .filter(|e| e.intent_type == "CLOSE_CHANNEL")
        .map(|e| e.target.clone())
        .collect();
    let mut final_list = Vec::with_capacity(filtered.len());
    for env in filtered {
        if env.intent_type == "REBALANCE" && closing_targets.contains(&env.target) {
            let detail = format!("target {} has a close intent", env.target);
            rejected.push((
                env,
                Code::ConflictCloseRebalance.as_str().to_string(),
                detail,
            ));
            continue;
        }
        final_list.push(env);
    }

    if extended_rules {
        // 5. Duplicate opens to one peer — best-sorted wins.
        let mut open_winner: HashMap<String, IntentEnvelope> = HashMap::new();
        let mut step5 = Vec::with_capacity(final_list.len());
        for env in final_list {
            if env.intent_type == "OPEN_CHANNEL" {
                if let Some(winner) = open_winner.get(&env.target) {
                    let detail = format!("target {} already has an open intent", env.target);
                    superseded.insert(
                        env.intent_id.as_str().to_string(),
                        winner.intent_id.as_str().to_string(),
                    );
                    rejected.push((
                        env,
                        Code::ConflictDuplicateOpen.as_str().to_string(),
                        detail,
                    ));
                    continue;
                }
                open_winner.insert(env.target.clone(), env.clone());
            }
            step5.push(env);
        }

        // 6. Circular rebalance vs structural swap on one channel — swap outranks.
        let swap_targets: HashSet<String> = step5
            .iter()
            .filter(|e| e.intent_type == "SWAP_OUT")
            .map(|e| e.target.clone())
            .collect();
        final_list = Vec::with_capacity(step5.len());
        for env in step5 {
            if env.intent_type == "REBALANCE" && swap_targets.contains(&env.target) {
                let detail = format!("target {} has a structural swap intent", env.target);
                rejected.push((
                    env,
                    Code::ConflictRebalanceSwap.as_str().to_string(),
                    detail,
                ));
                continue;
            }
            final_list.push(env);
        }
    }

    ArbitrationResult {
        ordered: final_list,
        rejected,
        superseded,
    }
}

/// Live arbitration state at the governor boundary (Phase 3F in the Python
/// history). Tracks unexpired in-flight intents so the governor can reject
/// conflicts BEFORE granting. LIVE semantics genuinely differ from
/// [`arbitrate`]'s batch semantics — see the module doc comment.
struct RegistryInner {
    active: HashMap<String, IntentEnvelope>,
}

impl RegistryInner {
    fn prune(&mut self, now: i64) {
        self.active.retain(|_, env| now < env.expires_at.value());
    }
}

pub struct ActiveIntentRegistry {
    inner: Mutex<RegistryInner>,
    extended_rules_provider: Option<Box<dyn Fn() -> bool + Send + Sync>>,
}

impl ActiveIntentRegistry {
    pub fn new(extended_rules_provider: Option<Box<dyn Fn() -> bool + Send + Sync>>) -> Self {
        ActiveIntentRegistry {
            inner: Mutex::new(RegistryInner {
                active: HashMap::new(),
            }),
            extended_rules_provider,
        }
    }

    /// `None` or a panicking provider means legacy rules only — fail to
    /// current (less strict) behavior, never to the stricter extended
    /// ruleset (mirrors Python's `try: provider() is True / except: False`).
    fn extended(&self) -> bool {
        match &self.extended_rules_provider {
            None => false,
            Some(provider) => {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(provider)).unwrap_or(false)
            }
        }
    }

    /// `Some(reason_code)` blocking, or `None` (then registered). Prunes
    /// `now >= expires_at` entries first.
    ///
    /// Legacy rules: duplicate idempotency key -> `INTENT_SUPERSEDED`;
    /// an incoming REBALANCE whose target has an active CLOSE_CHANNEL ->
    /// `CONFLICT_CLOSE_REBALANCE`. Extended rules (only when the provider
    /// says so): an incoming OPEN_CHANNEL whose target already has an
    /// active OPEN_CHANNEL -> `CONFLICT_DUPLICATE_OPEN` (first-registered
    /// wins); an incoming REBALANCE/SWAP_OUT whose target has an active
    /// intent of the *other* of that pair -> `CONFLICT_REBALANCE_SWAP`
    /// (either direction blocks the second arrival — this is a LIVE-only
    /// asymmetry: unlike batch, a CLOSE_CHANNEL arriving after an already-
    /// active REBALANCE is never itself blocked).
    pub fn check_and_register(&self, env: &IntentEnvelope, now: i64) -> Option<&'static str> {
        let mut inner = self.inner.lock().unwrap();
        inner.prune(now);

        if inner.active.contains_key(&env.idempotency_key) {
            return Some(Code::IntentSuperseded.as_str());
        }

        if env.intent_type == "REBALANCE" {
            for active in inner.active.values() {
                if active.intent_type == "CLOSE_CHANNEL" && active.target == env.target {
                    return Some(Code::ConflictCloseRebalance.as_str());
                }
            }
        }

        if self.extended() {
            if env.intent_type == "OPEN_CHANNEL" {
                for active in inner.active.values() {
                    if active.intent_type == "OPEN_CHANNEL" && active.target == env.target {
                        return Some(Code::ConflictDuplicateOpen.as_str());
                    }
                }
            }
            if env.intent_type == "REBALANCE" || env.intent_type == "SWAP_OUT" {
                let other = if env.intent_type == "REBALANCE" {
                    "SWAP_OUT"
                } else {
                    "REBALANCE"
                };
                for active in inner.active.values() {
                    if active.intent_type == other && active.target == env.target {
                        return Some(Code::ConflictRebalanceSwap.as_str());
                    }
                }
            }
        }

        inner
            .active
            .insert(env.idempotency_key.clone(), env.clone());
        None
    }

    pub fn release(&self, idempotency_key: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.active.remove(idempotency_key);
    }

    pub fn active_count(&self, now: i64) -> usize {
        let mut inner = self.inner.lock().unwrap();
        inner.prune(now);
        inner.active.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intents::{make_intent, Explanation, IntentFields};
    use crate::types::{Micro, Msat, SignedMsat, UnixTime};

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

    /// Mirrors the generator's `_env()` helper (`tools/conformance/
    /// generate_scenarios.py`), including its "amount 0 is falsy -> no
    /// amount_msat" and "capital defaults to amount" quirks.
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

    // --- precedence_class ---

    #[test]
    fn precedence_class_orders_types_per_spec_table() {
        let of_type = |t: &'static str| {
            env(Args {
                intent_type: t,
                ..Default::default()
            })
        };
        assert_eq!(precedence_class(&of_type("MAINTAIN_ONCHAIN_RESERVE")), 1);
        assert_eq!(precedence_class(&of_type("CLOSE_CHANNEL")), 3);
        assert_eq!(precedence_class(&of_type("SET_FEE")), 4);
        assert_eq!(precedence_class(&of_type("SET_HTLC_MAX")), 4);
        assert_eq!(precedence_class(&of_type("REBALANCE")), 5);
        assert_eq!(precedence_class(&of_type("SWAP_IN")), 5);
        assert_eq!(precedence_class(&of_type("SWAP_OUT")), 5);
        assert_eq!(precedence_class(&of_type("OPEN_CHANNEL")), 6);
        assert_eq!(precedence_class(&of_type("JOIN_LIQUIDITY_SWAP")), 6);
    }

    // --- arbitrate: staleness ---

    #[test]
    fn arbitrate_rejects_stale_intent_inclusive_boundary() {
        let stale = env(Args {
            created: NOW - 700,
            expires: Some(NOW - 100),
            ..Default::default()
        });
        let stale_id = stale.intent_id.clone();
        let result = arbitrate(std::slice::from_ref(&stale), NOW, false);
        assert!(result.ordered.is_empty());
        assert_eq!(result.rejected.len(), 1);
        assert_eq!(result.rejected[0].0.intent_id, stale_id);
        assert_eq!(result.rejected[0].1, "INTENT_STALE");
        assert_eq!(result.rejected[0].2, "expired before arbitration");
    }

    // --- ActiveIntentRegistry: basic + legacy conflict ---

    #[test]
    fn registry_blocks_duplicate_idempotency_key() {
        let registry = ActiveIntentRegistry::new(None);
        let a = env(Args::default());
        let b = env(Args::default()); // identical five-field subset -> same key
        assert_eq!(registry.check_and_register(&a, NOW), None);
        assert_eq!(
            registry.check_and_register(&b, NOW),
            Some("INTENT_SUPERSEDED")
        );
        assert_eq!(registry.active_count(NOW), 1);
    }

    #[test]
    fn registry_prunes_expired_before_checking() {
        let registry = ActiveIntentRegistry::new(None);
        let a = env(Args {
            created: NOW,
            expires: Some(NOW + 600),
            ..Default::default()
        });
        assert_eq!(registry.check_and_register(&a, NOW), None);
        assert_eq!(registry.active_count(NOW + 599), 1);
        assert_eq!(registry.active_count(NOW + 600), 0); // inclusive boundary
    }

    #[test]
    fn registry_release_frees_the_slot() {
        let registry = ActiveIntentRegistry::new(None);
        let a = env(Args::default());
        assert_eq!(registry.check_and_register(&a, NOW), None);
        registry.release(&a.idempotency_key);
        assert_eq!(registry.active_count(NOW), 0);
        // Re-registering the same key now succeeds.
        assert_eq!(registry.check_and_register(&a, NOW), None);
    }

    /// LIVE-vs-BATCH divergence, direction 1: a REBALANCE arriving while a
    /// CLOSE_CHANNEL on the same target is already active is blocked, same
    /// as batch would reject it.
    #[test]
    fn registry_close_then_rebalance_blocks_the_rebalance() {
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

    /// LIVE-vs-BATCH divergence, direction 2: the reverse arrival order is
    /// NOT blocked live — a CLOSE_CHANNEL arriving after an already-active
    /// REBALANCE on the same target registers cleanly. Batch, by contrast,
    /// always rejects the REBALANCE once any CLOSE_CHANNEL exists among the
    /// candidates, regardless of original input order (it sorts first).
    /// This asymmetry is the point of having two separate implementations —
    /// never unify them.
    #[test]
    fn registry_rebalance_then_close_does_not_block_the_close() {
        let registry = ActiveIntentRegistry::new(None);
        let reb = env(Args {
            intent_type: "REBALANCE",
            target: "222x333x0".to_string(),
            ..Default::default()
        });
        let close = env(Args {
            intent_type: "CLOSE_CHANNEL",
            target: "222x333x0".to_string(),
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

    // --- ActiveIntentRegistry: extended rules ---

    #[test]
    fn registry_extended_blocks_duplicate_open_first_registered_wins() {
        let registry = ActiveIntentRegistry::new(Some(Box::new(|| true)));
        let peer = "02".to_string() + &"b".repeat(64);
        let first = env(Args {
            intent_type: "OPEN_CHANNEL",
            target: peer.clone(),
            amount_sats: 1_000_000,
            bucket: "channel_open",
            policy: "planner",
            priority: 50,
            ..Default::default()
        });
        let second = env(Args {
            intent_type: "OPEN_CHANNEL",
            target: peer,
            amount_sats: 2_000_000,
            bucket: "channel_open",
            policy: "lnplus_lifecycle_governed",
            priority: 80,
            ..Default::default()
        });
        assert_eq!(registry.check_and_register(&first, NOW), None);
        assert_eq!(
            registry.check_and_register(&second, NOW),
            Some("CONFLICT_DUPLICATE_OPEN")
        );
    }

    #[test]
    fn registry_extended_blocks_rebalance_swap_either_direction() {
        // Direction A: REBALANCE active, SWAP_OUT arrives second.
        let registry_a = ActiveIntentRegistry::new(Some(Box::new(|| true)));
        let reb = env(Args {
            intent_type: "REBALANCE",
            target: "111x222x0".to_string(),
            ..Default::default()
        });
        let swap = env(Args {
            intent_type: "SWAP_OUT",
            target: "111x222x0".to_string(),
            amount_sats: 250_000,
            bucket: "rebalance",
            policy: "boltz",
            ..Default::default()
        });
        assert_eq!(registry_a.check_and_register(&reb, NOW), None);
        assert_eq!(
            registry_a.check_and_register(&swap, NOW),
            Some("CONFLICT_REBALANCE_SWAP")
        );

        // Direction B: SWAP_OUT active, REBALANCE arrives second.
        let registry_b = ActiveIntentRegistry::new(Some(Box::new(|| true)));
        let swap2 = env(Args {
            intent_type: "SWAP_OUT",
            target: "444x555x0".to_string(),
            amount_sats: 250_000,
            bucket: "rebalance",
            policy: "boltz",
            ..Default::default()
        });
        let reb2 = env(Args {
            intent_type: "REBALANCE",
            target: "444x555x0".to_string(),
            ..Default::default()
        });
        assert_eq!(registry_b.check_and_register(&swap2, NOW), None);
        assert_eq!(
            registry_b.check_and_register(&reb2, NOW),
            Some("CONFLICT_REBALANCE_SWAP")
        );
    }

    #[test]
    fn registry_extended_rules_not_applied_without_provider() {
        let registry = ActiveIntentRegistry::new(None);
        let reb = env(Args {
            intent_type: "REBALANCE",
            target: "111x222x0".to_string(),
            ..Default::default()
        });
        let swap = env(Args {
            intent_type: "SWAP_OUT",
            target: "111x222x0".to_string(),
            amount_sats: 250_000,
            bucket: "rebalance",
            policy: "boltz",
            ..Default::default()
        });
        assert_eq!(registry.check_and_register(&reb, NOW), None);
        // No extended rules -> the SWAP_OUT/REBALANCE conflict is not enforced.
        assert_eq!(registry.check_and_register(&swap, NOW), None);
    }

    #[test]
    fn registry_provider_panic_falls_back_to_legacy_never_stricter() {
        let registry = ActiveIntentRegistry::new(Some(Box::new(|| panic!("boom"))));
        let reb = env(Args {
            intent_type: "REBALANCE",
            target: "111x222x0".to_string(),
            ..Default::default()
        });
        let swap = env(Args {
            intent_type: "SWAP_OUT",
            target: "111x222x0".to_string(),
            amount_sats: 250_000,
            bucket: "rebalance",
            policy: "boltz",
            ..Default::default()
        });
        assert_eq!(registry.check_and_register(&reb, NOW), None);
        // Provider errored -> legacy rules only, same as `None`.
        assert_eq!(registry.check_and_register(&swap, NOW), None);
    }
}
