//! Integration tests for `revops_econ::governor`, porting the relevant
//! parts of `modules/governor_facade.py`'s behavior plus the conformance
//! corpus scenarios named in the Task 7 brief:
//!
//! - scenario 22 (`22-budget-exhaustion/case.json`): reservation refused ->
//!   `BUDGET_EXHAUSTED`, no spend.
//! - scenario 29 (`29-lnplus-obligation-lower-authority/case.json`): TWO
//!   legs against the same obligation envelope — gated (observe vs required
//!   capital) -> `AUTHORITY_LEVEL_BLOCKED`; ungated -> authorized.
//! - scenario 30 (`30-stale-intent/case.json`): stale envelope rejected
//!   fail-closed -> `INTENT_STALE`.
//!
//! Corpus VALUES are transcribed here per repo convention (copy the values
//! into tests; the corpus itself is not vendored) from
//! `cl_revenue_ops-port/tests/conformance/scenarios/{22,29,30}-*/case.json`.
//! Decision dicts (`{authorized, reason_code}`) are compared field-for-field
//! against each `expected`/`if_authority_gated`/`lnplus_path_is_ungated`
//! block — this is the same shape `_decision_wire` in
//! `tools/conformance/generate_scenarios.py` produces, so a field-for-field
//! match here is byte-equal to the Python reference under canonical JSON.

use revops_econ::arbiter::ActiveIntentRegistry;
use revops_econ::governor::{authority_allows, AuthorizationToken, GovernorFacade};
use revops_econ::intents::{
    from_wire, make_intent, to_wire, Explanation, IntentEnvelope, IntentFields,
};
use revops_econ::ledger::EconLedger;
use revops_econ::types::{EconResult, Micro, Msat, SignedMsat, UnixTime};
use tempfile::TempDir;

const NOW: i64 = 1_752_400_000;

struct Args {
    intent_type: &'static str,
    target: String,
    amount_sats: i64,
    max_cost_sats: i64,
    capital_sats: Option<i64>,
    priority: i32,
    bucket: &'static str,
    reason_codes: Vec<String>,
    created: i64,
    expires: Option<i64>,
}

impl Default for Args {
    fn default() -> Self {
        Args {
            intent_type: "REBALANCE",
            target: "111x222x0".to_string(),
            amount_sats: 400_000,
            max_cost_sats: 3_000,
            capital_sats: None,
            priority: 50,
            bucket: "rebalance",
            reason_codes: vec![],
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
        expected_benefit_msat: SignedMsat(0),
        max_cost_msat: Msat::new(args.max_cost_sats * 1000).unwrap(),
        capital_committed_msat: Msat::new(capital_sats * 1000).unwrap(),
        confidence_micro: Micro::new(0).unwrap(),
        reason_codes: args.reason_codes,
        explanation: Explanation {
            kind: "conformance".to_string(),
            components: vec![("case".to_string(), serde_json::json!(1))],
        },
        preconditions: vec![],
        priority: args.priority,
        budget_bucket: args.bucket.to_string(),
        origin_policy: "conformance".to_string(),
        reversible: false,
    })
    .unwrap()
}

fn noop_reserve(_rid: &str, _amount: i64, _category: &str) -> EconResult<bool> {
    Ok(true)
}

fn refusing_reserve(_rid: &str, _amount: i64, _category: &str) -> EconResult<bool> {
    Ok(false)
}

fn noop_release(_rid: &str) -> EconResult<bool> {
    Ok(true)
}

fn not_paused() -> bool {
    false
}

fn paused() -> bool {
    true
}

// --- corpus scenario 22: 22-budget-exhaustion ---
//
// `tests/conformance/scenarios/22-budget-exhaustion/case.json`: REBALANCE,
// target 111x222x0, amount 400_000 sats, max_cost 3_000 sats, bucket
// "rebalance"; `reserve_delegate_grants: false`; called with
// `reservation_id="r-1"`. Expected: `{authorized: false, reason_code:
// "BUDGET_EXHAUSTED"}`.
#[test]
fn corpus_s22_budget_exhaustion_refused() {
    let env = env(Args::default());
    assert_eq!(
        env.idempotency_key,
        "1119af2f103eb34445d30decf78b97f16d11267a8fe66e6aa83b340abdbe33f4"
    );
    let facade = GovernorFacade {
        reserve_spend: &refusing_reserve,
        release_spend: &noop_release,
        is_paused: &not_paused,
        ledger: None,
        registry: None,
        authority_check: None,
    };
    let decision = facade.authorize(&env, NOW, Some("r-1")).unwrap();
    assert!(!decision.authorized);
    assert!(decision.token.is_none());
    assert_eq!(decision.reason_code, "BUDGET_EXHAUSTED");
}

// --- corpus scenario 30: 30-stale-intent ---
//
// REBALANCE created NOW-700, expires NOW-100, authorized at NOW with
// reservation_id "r-stale". Expected: `{authorized: false, reason_code:
// "INTENT_STALE"}`.
#[test]
fn corpus_s30_stale_intent_rejected() {
    let env = env(Args {
        created: NOW - 700,
        expires: Some(NOW - 100),
        ..Default::default()
    });
    let facade = GovernorFacade {
        reserve_spend: &noop_reserve,
        release_spend: &noop_release,
        is_paused: &not_paused,
        ledger: None,
        registry: None,
        authority_check: None,
    };
    let decision = facade.authorize(&env, NOW, Some("r-stale")).unwrap();
    assert!(!decision.authorized);
    assert_eq!(decision.reason_code, "INTENT_STALE");
}

// --- corpus scenario 29: 29-lnplus-obligation-lower-authority ---
//
// OPEN_CHANNEL obligation (reason_codes=[CONTRACT_OBLIGATION]), amount
// 0 (falsy -> no amount_msat), max_cost 214 sats, capital 2_000_000
// sats, bucket "channel_open", priority 80, target a synthetic 66-char
// peer id. Two legs against the SAME envelope:
//  - if_authority_gated: authority_check = authority_allows("observe",
//    "capital") = false -> AUTHORITY_LEVEL_BLOCKED.
//  - lnplus_path_is_ungated: no authority_check -> authorized (reserve
//    delegate grants by default) with reason_code "".
#[test]
fn corpus_s29_lnplus_obligation_under_lower_authority() {
    let peer = format!("02{}", "b".repeat(64));
    let obligation = env(Args {
        intent_type: "OPEN_CHANNEL",
        target: peer,
        amount_sats: 0,
        max_cost_sats: 214,
        capital_sats: Some(2_000_000),
        priority: 80,
        bucket: "channel_open",
        reason_codes: vec!["CONTRACT_OBLIGATION".to_string()],
        ..Default::default()
    });
    assert_eq!(
        obligation.idempotency_key,
        "4ef5f723031e93afe9520809f759f47f2ff5d5a41d9264e222780812bdf18fd3"
    );
    assert!(obligation.amount_msat.is_none());

    let gated_check = || -> EconResult<bool> { Ok(authority_allows(Some("observe"), "capital")) };
    let gated_facade = GovernorFacade {
        reserve_spend: &noop_reserve,
        release_spend: &noop_release,
        is_paused: &not_paused,
        ledger: None,
        registry: None,
        authority_check: Some(&gated_check),
    };
    let blocked = gated_facade
        .authorize(&obligation, NOW, Some("lnplus-1"))
        .unwrap();
    assert!(!blocked.authorized);
    assert_eq!(blocked.reason_code, "AUTHORITY_LEVEL_BLOCKED");

    let ungated_facade = GovernorFacade {
        reserve_spend: &noop_reserve,
        release_spend: &noop_release,
        is_paused: &not_paused,
        ledger: None,
        registry: None,
        authority_check: None,
    };
    let ungated = ungated_facade
        .authorize(&obligation, NOW, Some("lnplus-1"))
        .unwrap();
    assert!(ungated.authorized);
    assert_eq!(ungated.reason_code, "");
    let token = ungated.token.expect("authorized decision carries a token");
    assert_eq!(token.reservation_id, "lnplus-1");
    assert_eq!(token.reserved_msat, 214_000);
}

// --- decision-order pins ---

#[test]
fn paused_beats_everything_including_authority_and_staleness() {
    // Stale envelope AND an authority_check that would also block --
    // paused must still win (it is checked first).
    let stale = env(Args {
        created: NOW - 700,
        expires: Some(NOW - 100),
        ..Default::default()
    });
    let never_allow = || -> EconResult<bool> { Ok(false) };
    let facade = GovernorFacade {
        reserve_spend: &noop_reserve,
        release_spend: &noop_release,
        is_paused: &paused,
        ledger: None,
        registry: None,
        authority_check: Some(&never_allow),
    };
    let decision = facade.authorize(&stale, NOW, None).unwrap();
    assert_eq!(decision.reason_code, "PAUSED");
}

#[test]
fn authority_beats_staleness() {
    // Stale envelope, authority_check blocks -> AUTHORITY_LEVEL_BLOCKED,
    // not INTENT_STALE (authority is checked before expiry).
    let stale = env(Args {
        created: NOW - 700,
        expires: Some(NOW - 100),
        ..Default::default()
    });
    let never_allow = || -> EconResult<bool> { Ok(false) };
    let facade = GovernorFacade {
        reserve_spend: &noop_reserve,
        release_spend: &noop_release,
        is_paused: &not_paused,
        ledger: None,
        registry: None,
        authority_check: Some(&never_allow),
    };
    let decision = facade.authorize(&stale, NOW, None).unwrap();
    assert_eq!(decision.reason_code, "AUTHORITY_LEVEL_BLOCKED");
}

#[test]
fn authority_check_err_fails_closed_to_blocked() {
    let erroring: &dyn Fn() -> EconResult<bool> = &|| {
        Err(revops_econ::types::EconError {
            msg: "boom".to_string(),
        })
    };
    let env = env(Args::default());
    let facade = GovernorFacade {
        reserve_spend: &noop_reserve,
        release_spend: &noop_release,
        is_paused: &not_paused,
        ledger: None,
        registry: None,
        authority_check: Some(erroring),
    };
    let decision = facade.authorize(&env, NOW, None).unwrap();
    assert!(!decision.authorized);
    assert_eq!(decision.reason_code, "AUTHORITY_LEVEL_BLOCKED");
}

// --- zero-cost intent: no reservation, no budget_reserved event ---

#[test]
fn zero_cost_intent_authorizes_without_reservation_or_budget_event() {
    let dir = TempDir::new().unwrap();
    let ledger = EconLedger::open(dir.path().join("l.db")).unwrap();

    // reserve_spend must NEVER be called for a zero-cost intent: wire a
    // delegate that always refuses, to prove the facade never invokes
    // it on this path.
    let env = env(Args {
        intent_type: "SET_FEE",
        target: "111x222x0".to_string(),
        amount_sats: 0,
        max_cost_sats: 0,
        capital_sats: Some(0),
        bucket: "fees",
        ..Default::default()
    });

    let facade = GovernorFacade {
        reserve_spend: &refusing_reserve,
        release_spend: &noop_release,
        is_paused: &not_paused,
        ledger: Some(&ledger),
        registry: None,
        authority_check: None,
    };
    let decision = facade.authorize(&env, NOW, Some("r-zero")).unwrap();
    assert!(decision.authorized);
    let token = decision.token.unwrap();
    assert_eq!(token.reserved_msat, 0);
    assert_eq!(token.reservation_id, "r-zero");

    assert_eq!(ledger.count_events(Some("intent_authorized")).unwrap(), 1);
    assert_eq!(ledger.count_events(Some("budget_reserved")).unwrap(), 0);
}

// --- ledger keying pin: intent_authorized under envelope key,
// budget_reserved under the caller's effective reservation id ---

#[test]
fn ledger_keying_pin_intent_authorized_vs_budget_reserved() {
    let dir = TempDir::new().unwrap();
    let ledger = EconLedger::open(dir.path().join("l.db")).unwrap();

    let env = env(Args::default());
    let facade = GovernorFacade {
        reserve_spend: &noop_reserve,
        release_spend: &noop_release,
        is_paused: &not_paused,
        ledger: Some(&ledger),
        registry: None,
        authority_check: None,
    };
    let decision = facade.authorize(&env, NOW, Some("r-1")).unwrap();
    assert!(decision.authorized);
    let token = decision.token.unwrap();
    assert_eq!(token.token_id, "auth-1119af2f103eb344");
    assert_eq!(token.reservation_id, "r-1");
    assert_eq!(token.reserved_msat, 3_000_000);

    let events = ledger.events(0).unwrap();
    let authorized_event = events
        .iter()
        .find(|e| e.event_type == "intent_authorized")
        .expect("intent_authorized event recorded");
    assert_eq!(authorized_event.idempotency_key, env.idempotency_key);

    let reserved_event = events
        .iter()
        .find(|e| e.event_type == "budget_reserved")
        .expect("budget_reserved event recorded");
    assert_eq!(reserved_event.idempotency_key, "r-1");
    assert_ne!(reserved_event.idempotency_key, env.idempotency_key);
    assert_eq!(
        *reserved_event.amounts.get("reserved_msat").unwrap(),
        3_000_000
    );
}

// --- release() ---

#[test]
fn release_calls_release_spend_and_ledgers_a_release_event() {
    let dir = TempDir::new().unwrap();
    let ledger = EconLedger::open(dir.path().join("l.db")).unwrap();

    let env = env(Args::default());
    let facade = GovernorFacade {
        reserve_spend: &noop_reserve,
        release_spend: &noop_release,
        is_paused: &not_paused,
        ledger: Some(&ledger),
        registry: None,
        authority_check: None,
    };
    let decision = facade.authorize(&env, NOW, Some("r-1")).unwrap();
    let token = decision.token.unwrap();

    let released = facade.release(&token, NOW + 10).unwrap();
    assert!(released);

    let events = ledger.events(0).unwrap();
    let release_event = events
        .iter()
        .find(|e| e.event_type == "reservation_released")
        .expect("reservation_released event recorded");
    assert_eq!(release_event.cycle_id, "release");
    assert_eq!(release_event.idempotency_key, "r-1");
    assert_eq!(
        *release_event.amounts.get("released_msat").unwrap(),
        3_000_000
    );
}

#[test]
fn release_releases_registry_slot_by_arbitration_key() {
    let registry = ActiveIntentRegistry::new(None);
    let env = env(Args::default());
    assert_eq!(registry.check_and_register(&env, NOW), None);
    assert_eq!(registry.active_count(NOW), 1);

    let facade = GovernorFacade {
        reserve_spend: &noop_reserve,
        release_spend: &noop_release,
        is_paused: &not_paused,
        ledger: None,
        registry: Some(&registry),
        authority_check: None,
    };
    let token = AuthorizationToken {
        token_id: "auth-xxxxxxxxxxxxxxxx".to_string(),
        intent_id: env.intent_id.as_str().to_string(),
        reservation_id: "r-1".to_string(),
        reserved_msat: 3_000_000,
        budget_bucket: "rebalance".to_string(),
        issued_at: NOW,
        arbitration_key: env.idempotency_key.clone(),
    };
    facade.release(&token, NOW).unwrap();
    assert_eq!(registry.active_count(NOW), 0);
}

// --- registry conflict path ---

#[test]
fn registry_conflict_blocks_with_conflict_code_and_ledgers_intent_rejected() {
    let dir = TempDir::new().unwrap();
    let ledger = EconLedger::open(dir.path().join("l.db")).unwrap();
    let registry = ActiveIntentRegistry::new(None);

    let a = env(Args::default());
    let b = env(Args::default()); // identical five-field subset -> same key
    assert_eq!(registry.check_and_register(&a, NOW), None);

    let facade = GovernorFacade {
        reserve_spend: &noop_reserve,
        release_spend: &noop_release,
        is_paused: &not_paused,
        ledger: Some(&ledger),
        registry: Some(&registry),
        authority_check: None,
    };
    let decision = facade.authorize(&b, NOW, Some("r-2")).unwrap();
    assert!(!decision.authorized);
    assert_eq!(decision.reason_code, "INTENT_SUPERSEDED");

    let events = ledger.events(0).unwrap();
    let rejected_event = events
        .iter()
        .find(|e| e.event_type == "intent_rejected")
        .expect("intent_rejected event recorded");
    assert_eq!(rejected_event.idempotency_key, b.idempotency_key);
    assert_eq!(rejected_event.details["reason_code"], "INTENT_SUPERSEDED");
    assert_eq!(rejected_event.details["arbitration"], true);
    // Never reserved on the rejected path.
    assert_eq!(ledger.count_events(Some("budget_reserved")).unwrap(), 0);
}

// --- pre-Phase-2b fix: idempotency_key[..16] panic on a short/multibyte
// key arriving via from_wire (governor.rs token_id construction) ---
//
// Python's `IntentEnvelope.__post_init__` (modules/econ_intents.py) does
// NOT validate `idempotency_key` shape at all, and Python string slicing
// (`key[:16]`) never raises regardless of length or content. Parity means
// Rust must not add a validation Python lacks; instead `authorize` uses a
// defensive `.get(..16).unwrap_or(&full_key)` byte slice. These tests drive
// a short/multibyte key through `from_wire` -> `authorize` and assert the
// call completes (no panic) rather than asserting a particular decision.

fn wire_with_idempotency_key(env: &IntentEnvelope, key: &str) -> IntentEnvelope {
    let mut wire = to_wire(env);
    wire["idempotency_key"] = serde_json::json!(key);
    from_wire(&wire).expect(
        "from_wire must accept any non-empty string key (no shape validation, matching Python)",
    )
}

#[test]
fn from_wire_accepts_short_idempotency_key_and_authorize_does_not_panic() {
    let base = env(Args::default());
    // 3 bytes: shorter than the 16-byte slice the old code took unconditionally.
    let short_key_env = wire_with_idempotency_key(&base, "abc");

    let facade = GovernorFacade {
        reserve_spend: &noop_reserve,
        release_spend: &noop_release,
        is_paused: &not_paused,
        ledger: None,
        registry: None,
        authority_check: None,
    };
    // Must not panic; the actual decision doesn't matter for this test.
    let decision = facade.authorize(&short_key_env, NOW, None).unwrap();
    assert!(decision.authorized);
    let token = decision.token.unwrap();
    assert_eq!(token.token_id, "auth-abc");
}

#[test]
fn from_wire_accepts_multibyte_boundary_idempotency_key_and_authorize_does_not_panic() {
    let base = env(Args::default());
    // 15 ASCII bytes + one 2-byte UTF-8 codepoint ('é'): byte offset 16
    // lands on the second byte of 'é', not a char boundary. The old
    // `&s[..16]` would panic here even though the string is >= 16 bytes.
    let multibyte_key = format!("{}é", "a".repeat(15));
    assert_eq!(multibyte_key.len(), 17); // 15 + 2 bytes for 'é'
    let mb_env = wire_with_idempotency_key(&base, &multibyte_key);

    let facade = GovernorFacade {
        reserve_spend: &noop_reserve,
        release_spend: &noop_release,
        is_paused: &not_paused,
        ledger: None,
        registry: None,
        authority_check: None,
    };
    // Must not panic; falls back to the whole key since byte 16 isn't a
    // char boundary.
    let decision = facade.authorize(&mb_env, NOW, None).unwrap();
    assert!(decision.authorized);
    let token = decision.token.unwrap();
    assert_eq!(token.token_id, format!("auth-{multibyte_key}"));
}

// --- pre-Phase-2b fix: unchecked money arithmetic (governor reserve site)
// ---
//
// `reserve_sats * 1000` used a bare multiply. `max_cost_msat` is only
// bounded to u63 (`[0, 2**63-1]`); `to_sats_ceil` can round a max-range
// value up just far enough that re-expanding to msat overflows i64 — this
// is reachable with a legitimately-constructed max-range envelope, not
// just adversarial input. `authorize` must return `Err`, not panic.
#[test]
fn authorize_reserve_site_max_range_overflow_returns_err() {
    let created_at = UnixTime::new(NOW).unwrap();
    let env = make_intent(IntentFields {
        intent_type: "REBALANCE".to_string(),
        snapshot_id: "snap-1".to_string(),
        created_at,
        expires_at: created_at.plus_seconds(600).unwrap(),
        target: "111x222x0".to_string(),
        amount_msat: None,
        expected_benefit_msat: SignedMsat(0),
        // i64::MAX msat ceils to a sat count whose * 1000 exceeds i64::MAX.
        max_cost_msat: Msat::new(i64::MAX).unwrap(),
        capital_committed_msat: Msat::new(0).unwrap(),
        confidence_micro: Micro::new(0).unwrap(),
        reason_codes: vec![],
        explanation: Explanation {
            kind: "conformance".to_string(),
            components: vec![],
        },
        preconditions: vec![],
        priority: 50,
        budget_bucket: "rebalance".to_string(),
        origin_policy: "conformance".to_string(),
        reversible: false,
    })
    .unwrap();

    let facade = GovernorFacade {
        reserve_spend: &noop_reserve, // grants the reservation
        release_spend: &noop_release,
        is_paused: &not_paused,
        ledger: None,
        registry: None,
        authority_check: None,
    };
    let result = facade.authorize(&env, NOW, Some("r-overflow"));
    assert!(
        result.is_err(),
        "expected Err on reserved_msat overflow, got {result:?}"
    );
}
