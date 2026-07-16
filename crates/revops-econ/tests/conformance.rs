//! Phase 2 Task 9 (Wave 4, GATE): the Rust fixture-runner against the
//! vendored conformance corpus (`fixtures/conformance/scenarios/**`, a
//! byte-for-byte copy of `cl_revenue_ops-port/tests/conformance/scenarios/`).
//!
//! Unlike the T3-T8 unit tests (which transcribed corpus VALUES into their
//! own fixtures per the "copy the values into tests; the corpus itself is
//! not vendored — that's Task 9's job" convention), every test below reads
//! the actual vendored JSON file at run time and drives the merged Phase 2
//! modules against it. Comparison is always `canonical_json(produced) ==
//! canonical_json(expected)` (byte parity, no fuzz) — see
//! [`assert_canon_eq`].
//!
//! Reference semantics mirrored from `tools/conformance/generate_scenarios.py`
//! (`_arb_wire`, `_decision_wire`, `_env`, pinned `NOW = 1_752_400_000`) and
//! the schema gate mirrored from `tools/conformance/validate_fixtures.py`.
//!
//! 18 econ-core scenarios are replayed byte-identically (Step 2); the other
//! 22 are schema-gated only, via the pinned, test-enforced [`DEFERRED`] skip
//! list (Step 3) — every scenario directory must be either replayed or
//! deferred, so adding scenario 41 breaks the build until triaged.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use revops_core::canonical::canonical_json;
use revops_econ::arbiter::{arbitrate, ArbitrationResult};
use revops_econ::governor::{authority_allows, GovernorDecision, GovernorFacade};
use revops_econ::intents::{
    from_wire, is_expired, make_intent, Explanation, IntentEnvelope, IntentFields,
};
use revops_econ::ledger::{EconLedger, LedgerState};
use revops_econ::reconcile::{reconcile, DbReservationState};
use revops_econ::types::{EconResult, Micro, Msat, SignedMsat, UnixTime};
use serde_json::{json, Value};
use tempfile::TempDir;

/// Pinned replay clock (`tools/conformance/generate_scenarios.py::NOW`).
const NOW: i64 = 1_752_400_000;

// ---------------------------------------------------------------------------
// Fixture plumbing
// ---------------------------------------------------------------------------

fn fixtures_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/conformance/scenarios")
}

fn read_json(path: &Path) -> Value {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

fn scenario_file(dir: &str, filename: &str) -> Value {
    read_json(&fixtures_root().join(dir).join(filename))
}

fn case_json(dir: &str) -> Value {
    scenario_file(dir, "case.json")
}

/// Byte-parity assert: both sides go through `canonical_json` before
/// comparing, matching the task contract exactly.
fn assert_canon_eq(produced: &Value, expected: &Value, label: &str) {
    let p = canonical_json(produced).expect("produced value is canonical-encodable");
    let e = canonical_json(expected).expect("expected value is canonical-encodable");
    assert_eq!(
        p, e,
        "{label}: canonical JSON mismatch\n  produced: {p}\n  expected: {e}"
    );
}

fn collect_relative_files(root: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, root: &Path, out: &mut Vec<PathBuf>) {
        for entry in
            std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()))
        {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                walk(&path, root, out);
            } else {
                out.push(path.strip_prefix(root).unwrap().to_path_buf());
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

/// `REVOPS_PY_CORPUS` override, else `~/bin/cl_revenue_ops-port/tests/
/// conformance/scenarios`. `None` if neither resolves to an existing
/// directory (the drift guard then no-ops rather than failing an
/// environment that simply doesn't have the Python worktree checked out).
fn resolve_source_dir() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("REVOPS_PY_CORPUS") {
        let pb = PathBuf::from(p);
        return pb.is_dir().then_some(pb);
    }
    let home = std::env::var("HOME").ok()?;
    let default = PathBuf::from(home).join("bin/cl_revenue_ops-port/tests/conformance/scenarios");
    default.is_dir().then_some(default)
}

// ---------------------------------------------------------------------------
// Step 1: corpus integrity (byte-identical vendoring + schema gate)
// ---------------------------------------------------------------------------

#[test]
fn corpus_is_byte_identical_to_source() {
    let Some(source) = resolve_source_dir() else {
        eprintln!("skip corpus_is_byte_identical_to_source: source corpus not found");
        return;
    };
    let vendored = fixtures_root();
    let source_files = collect_relative_files(&source);
    let vendored_files = collect_relative_files(&vendored);
    assert_eq!(
        source_files, vendored_files,
        "vendored file set differs from source (drift in file presence, either direction)"
    );
    for rel in &source_files {
        let a = std::fs::read(source.join(rel)).unwrap();
        let b = std::fs::read(vendored.join(rel)).unwrap();
        assert_eq!(a, b, "byte drift in {}", rel.display());
    }
}

const KNOWN_SCHEMAS: &[(&str, i64)] = &[
    ("conformance_case", 0),
    ("ledger_event", 0),
    ("ledger_projection", 0),
];

#[test]
fn forty_scenarios_present_and_schema_gated() {
    let root = fixtures_root();
    let mut dirs: Vec<String> = std::fs::read_dir(&root)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    dirs.sort();
    assert_eq!(
        dirs.len(),
        40,
        "expected 40 scenario directories, found {}",
        dirs.len()
    );
    assert!(dirs[0].starts_with("01-"));
    assert!(dirs[39].starts_with("40-"));

    for dir in &dirs {
        let dir_path = root.join(dir);
        for entry in std::fs::read_dir(&dir_path).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let payload = read_json(&path);
            let name = payload.get("schema_name").and_then(Value::as_str);
            let version = payload.get("schema_version").and_then(Value::as_i64);
            let known = matches!((name, version),
                (Some(n), Some(v)) if KNOWN_SCHEMAS.iter().any(|(kn, kv)| *kn == n && *kv == v));
            assert!(
                known,
                "{}: unknown schema {:?} v{:?}",
                path.display(),
                name,
                version
            );
        }

        let case = case_json(dir);
        assert!(!case["inputs"].is_null(), "{dir}: inputs must not be null");
        let expected_nonempty = match &case["expected"] {
            Value::Object(m) => !m.is_empty(),
            Value::Null => false,
            _ => true,
        };
        assert!(expected_nonempty, "{dir}: expected must be non-empty");
    }
}

// ---------------------------------------------------------------------------
// Step 3: pinned replay / deferred partition
// ---------------------------------------------------------------------------

/// The 18 econ-core scenarios replayed byte-identically in this file.
const REPLAYED: &[&str] = &[
    "18-conflicting-close-rebalance",
    "20-open-vs-lnplus",
    "21-circular-vs-boltz-structural",
    "22-budget-exhaustion",
    "25-missing-execution-cost",
    "26-unknown-execution-outcome",
    "27-boltz-timeout-after-acceptance",
    "29-lnplus-obligation-lower-authority",
    "30-stale-intent",
    "31-duplicate-idempotency-key",
    "32-numeric-overflow-underflow",
    "33-msat-rounding-boundaries",
    "34-expired-intent",
    "35-stable-ordering-tiebreak",
    "36-map-order-independence",
    "37-clock-seed-determinism",
    "38-partial-batch-completion",
    "40-sanitized-production-decisions",
];

/// Explicit, never-silent skip list: the 22 scenarios owned by later phases.
/// Adding scenario 41 without triaging it into either this list or
/// [`REPLAYED`] fails [`all_scenarios_replayed_or_deferred`].
const DEFERRED: &[(&str, &str)] = &[
    (
        "01-ordinary-profitable-channel",
        "Phase 3: profitability classification",
    ),
    (
        "02-source-gateway-protection",
        "Phase 3: profitability classification (inbound-gateway close protection)",
    ),
    ("03-sink-depletion", "Phase 4: rebalance_mode planner"),
    (
        "04-balanced-channel",
        "Phase 3: profitability classification",
    ),
    (
        "05-underwater-classification",
        "Phase 3: profitability classification",
    ),
    (
        "06-stagnant-candidate",
        "Phase 3: profitability classification",
    ),
    (
        "07-zombie-classification",
        "Phase 3: profitability classification",
    ),
    ("08-fee-rail", "Phase 3: fee_stage controller (rails)"),
    (
        "09-fee-rate-limit",
        "Phase 3: fee_stage controller (rate limit)",
    ),
    (
        "10-fee-deadband",
        "Phase 3: fee_stage controller (deadband)",
    ),
    (
        "11-fee-cooldown",
        "Phase 3: fee_stage controller (cooldown)",
    ),
    (
        "12-dts-pid-components",
        "Phase 3: fee_stage controller (DTS/PID)",
    ),
    (
        "13-dynamic-htlcmax",
        "Phase 4: admission control (htlc_max)",
    ),
    (
        "14-hot-channel-priority",
        "Phase 4: rebalance_mode planner (priorities)",
    ),
    ("15-normal-rebalance", "Phase 4: rebalance_mode planner"),
    (
        "16-structural-drain",
        "Phase 4: rebalance_mode planner (Boltz)",
    ),
    (
        "17-manual-diagnostic-rebalance",
        "Phase 4: rebalance_mode planner",
    ),
    (
        "19-protected-close-rejection",
        "Phase 3-5: close-protection gate (protect tags)",
    ),
    (
        "23-concurrent-reservation-contention",
        "Phase 3: budget reservation rail (Database.reserve_spend)",
    ),
    (
        "24-restart-outstanding-reservation",
        "Phase 3: budget reservation rail (Database restart durability)",
    ),
    (
        "28-lnplus-state-divergence",
        "Phase 6: LN+ lifecycle module",
    ),
    (
        "39-bookkeeper-present-absent",
        "Prose-only contract (profitability_analyzer bookkeeper source) — no code path to replay",
    ),
];

#[test]
fn all_scenarios_replayed_or_deferred() {
    assert_eq!(REPLAYED.len(), 18, "expected exactly 18 replayed scenarios");
    assert_eq!(DEFERRED.len(), 22, "expected exactly 22 deferred scenarios");

    let mut dirs: Vec<String> = std::fs::read_dir(fixtures_root())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    dirs.sort();
    assert_eq!(dirs.len(), 40);

    for dir in &dirs {
        let replayed = REPLAYED.contains(&dir.as_str());
        let deferred = DEFERRED.iter().any(|(name, _)| name == dir);
        assert!(
            replayed || deferred,
            "{dir}: neither replayed nor in the pinned DEFERRED skip list — triage it"
        );
        assert!(
            !(replayed && deferred),
            "{dir}: listed as BOTH replayed and deferred"
        );
    }
    for name in REPLAYED {
        assert!(
            dirs.contains(&name.to_string()),
            "REPLAYED names a nonexistent dir: {name}"
        );
    }
    for (name, _) in DEFERRED {
        assert!(
            dirs.contains(&name.to_string()),
            "DEFERRED names a nonexistent dir: {name}"
        );
    }
}

// ---------------------------------------------------------------------------
// Step 2: replay dispatch
// ---------------------------------------------------------------------------

// --- arbitration (18, 20, 21, 31, 35, 38) ---

fn fields_from_wire(w: &Value) -> IntentFields {
    let amount_msat = match w.get("amount_msat") {
        None | Some(Value::Null) => None,
        Some(v) => Some(Msat::new(v.as_i64().expect("amount_msat integer")).unwrap()),
    };
    let explanation = &w["explanation"];
    let components = explanation["components"]
        .as_array()
        .expect("explanation.components array")
        .iter()
        .map(|pair| {
            let arr = pair.as_array().expect("[name, value] pair");
            (
                arr[0].as_str().expect("component name").to_string(),
                arr[1].clone(),
            )
        })
        .collect();
    IntentFields {
        intent_type: w["intent_type"].as_str().unwrap().to_string(),
        snapshot_id: w["snapshot_id"].as_str().unwrap().to_string(),
        created_at: UnixTime::new(w["created_at"].as_i64().unwrap()).unwrap(),
        expires_at: UnixTime::new(w["expires_at"].as_i64().unwrap()).unwrap(),
        target: w["target"].as_str().unwrap().to_string(),
        amount_msat,
        expected_benefit_msat: SignedMsat(w["expected_benefit_msat"].as_i64().unwrap()),
        max_cost_msat: Msat::new(w["max_cost_msat"].as_i64().unwrap()).unwrap(),
        capital_committed_msat: Msat::new(w["capital_committed_msat"].as_i64().unwrap()).unwrap(),
        confidence_micro: Micro::new(w["confidence_micro"].as_i64().unwrap()).unwrap(),
        reason_codes: w["reason_codes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect(),
        explanation: Explanation {
            kind: explanation["kind"].as_str().unwrap().to_string(),
            components,
        },
        preconditions: w["preconditions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect(),
        priority: i32::try_from(w["priority"].as_i64().unwrap()).unwrap(),
        budget_bucket: w["budget_bucket"].as_str().unwrap().to_string(),
        origin_policy: w["origin_policy"].as_str().unwrap().to_string(),
        reversible: w["reversible"].as_bool().unwrap(),
    }
}

fn arb_wire(result: &ArbitrationResult) -> Value {
    json!({
        "ordered_intent_ids": result
            .ordered
            .iter()
            .map(|e| e.intent_id.as_str().to_string())
            .collect::<Vec<_>>(),
        "rejected": result
            .rejected
            .iter()
            .map(|(e, rc, ck)| json!({
                "intent_id": e.intent_id.as_str(),
                "reason_code": rc,
                "conflicting_key": ck,
            }))
            .collect::<Vec<_>>(),
    })
}

/// Data-driven arbitration replay: parses `inputs.intents` from the
/// vendored fixture, reconstructs each envelope two independent ways
/// (`from_wire` directly, and `make_intent` from the same wire fields) and
/// asserts they agree — the cross-language idempotency-key/intent_id
/// parity check the task brief calls for — then runs [`arbitrate`] and
/// compares the `_arb_wire` shape against `expected` byte-for-byte.
fn run_arbitration_scenario(dir: &str) {
    let case = case_json(dir);
    let inputs = &case["inputs"];
    let intents_wire = inputs["intents"].as_array().expect("inputs.intents array");

    let mut envs: Vec<IntentEnvelope> = Vec::with_capacity(intents_wire.len());
    for w in intents_wire {
        let parsed = from_wire(w).unwrap_or_else(|e| panic!("{dir}: from_wire: {e}"));
        let rebuilt =
            make_intent(fields_from_wire(w)).unwrap_or_else(|e| panic!("{dir}: make_intent: {e}"));
        assert_eq!(
            rebuilt.intent_id, parsed.intent_id,
            "{dir}: make_intent-derived intent_id must match the wire's own intent_id"
        );
        assert_eq!(
            rebuilt.idempotency_key, parsed.idempotency_key,
            "{dir}: make_intent-derived idempotency_key must match the wire's own idempotency_key"
        );
        envs.push(parsed);
    }

    let extended = inputs
        .get("extended_rules")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let result = arbitrate(&envs, NOW, extended);
    assert_canon_eq(&arb_wire(&result), &case["expected"], dir);
}

#[test]
fn scenario_18_conflicting_close_rebalance() {
    run_arbitration_scenario("18-conflicting-close-rebalance");
}

#[test]
fn scenario_20_open_vs_lnplus() {
    run_arbitration_scenario("20-open-vs-lnplus");
}

#[test]
fn scenario_21_circular_vs_boltz_structural() {
    run_arbitration_scenario("21-circular-vs-boltz-structural");
}

#[test]
fn scenario_31_duplicate_idempotency_key() {
    run_arbitration_scenario("31-duplicate-idempotency-key");
}

#[test]
fn scenario_35_stable_ordering_tiebreak() {
    run_arbitration_scenario("35-stable-ordering-tiebreak");
}

#[test]
fn scenario_38_partial_batch_completion() {
    run_arbitration_scenario("38-partial-batch-completion");
}

// --- authorization (22, 29, 30) ---

fn decision_wire(d: &GovernorDecision) -> Value {
    json!({"authorized": d.authorized, "reason_code": d.reason_code})
}

fn always_ok_reserve(_rid: &str, _amount: i64, _category: &str) -> EconResult<bool> {
    Ok(true)
}

fn always_ok_release(_rid: &str) -> EconResult<bool> {
    Ok(true)
}

fn not_paused() -> bool {
    false
}

#[test]
fn scenario_22_budget_exhaustion() {
    let case = case_json("22-budget-exhaustion");
    let inputs = &case["inputs"];
    let env = from_wire(&inputs["intent"]).expect("intent wire parses");
    let grants = inputs["reserve_delegate_grants"]
        .as_bool()
        .expect("reserve_delegate_grants bool");

    let reserve_spend =
        |_rid: &str, _amount: i64, _category: &str| -> EconResult<bool> { Ok(grants) };
    let facade = GovernorFacade {
        reserve_spend: &reserve_spend,
        release_spend: &always_ok_release,
        is_paused: &not_paused,
        ledger: None,
        registry: None,
        authority_check: None,
    };
    let decision = facade.authorize(&env, NOW, Some("r-1")).unwrap();
    assert_canon_eq(
        &decision_wire(&decision),
        &case["expected"],
        "22-budget-exhaustion",
    );
}

#[test]
fn scenario_29_lnplus_obligation_lower_authority() {
    let case = case_json("29-lnplus-obligation-lower-authority");
    let inputs = &case["inputs"];
    let obligation = from_wire(&inputs["intent"]).expect("intent wire parses");
    let authority_level = inputs["authority_level"]
        .as_str()
        .expect("authority_level string")
        .to_string();

    let gated_check =
        move || -> EconResult<bool> { Ok(authority_allows(Some(&authority_level), "capital")) };
    let gated_facade = GovernorFacade {
        reserve_spend: &always_ok_reserve,
        release_spend: &always_ok_release,
        is_paused: &not_paused,
        ledger: None,
        registry: None,
        authority_check: Some(&gated_check),
    };
    let blocked = gated_facade
        .authorize(&obligation, NOW, Some("lnplus-1"))
        .unwrap();

    let ungated_facade = GovernorFacade {
        reserve_spend: &always_ok_reserve,
        release_spend: &always_ok_release,
        is_paused: &not_paused,
        ledger: None,
        registry: None,
        authority_check: None,
    };
    let ungated = ungated_facade
        .authorize(&obligation, NOW, Some("lnplus-1"))
        .unwrap();

    // The prose `invariant` field is copied through from `expected`, not
    // computed (mirrors the task brief exactly).
    let produced = json!({
        "if_authority_gated": decision_wire(&blocked),
        "lnplus_path_is_ungated": decision_wire(&ungated),
        "invariant": case["expected"]["invariant"].clone(),
    });
    assert_canon_eq(
        &produced,
        &case["expected"],
        "29-lnplus-obligation-lower-authority",
    );
}

#[test]
fn scenario_30_stale_intent() {
    let case = case_json("30-stale-intent");
    let inputs = &case["inputs"];
    let env = from_wire(&inputs["intent"]).expect("intent wire parses");
    let now = inputs.get("now").and_then(Value::as_i64).unwrap_or(NOW);

    let facade = GovernorFacade {
        reserve_spend: &always_ok_reserve,
        release_spend: &always_ok_release,
        is_paused: &not_paused,
        ledger: None,
        registry: None,
        authority_check: None,
    };
    let decision = facade.authorize(&env, now, Some("r-stale")).unwrap();
    assert_canon_eq(
        &decision_wire(&decision),
        &case["expected"],
        "30-stale-intent",
    );
}

// --- ledger (25, 26) + production_capture (40) ---

fn amounts_from_value(v: Option<&Value>) -> BTreeMap<String, i64> {
    v.and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, val)| val.as_i64().map(|n| (k.clone(), n)))
                .collect()
        })
        .unwrap_or_default()
}

/// Replays a `ledger_event`-shaped wire array (used by both `inputs.events`
/// and `expected-ledger-events.json`'s `expected.events`) into a fresh
/// tempdir ledger. Missing `intent_id`/`cycle_id`/`at`/`details` fall back
/// to fixed placeholders (mirrors the minimal wire shape scenario 25 uses),
/// while scenario 40's full wire supplies every field explicitly.
fn replay_ledger_events(events: &[Value]) -> LedgerState {
    let dir = TempDir::new().unwrap();
    let ledger = EconLedger::open(dir.path().join("l.db")).unwrap();
    for e in events {
        let event_type = e["event_type"].as_str().expect("event_type string");
        let key = e["idempotency_key"]
            .as_str()
            .expect("idempotency_key string");
        let amounts = amounts_from_value(e.get("amounts"));
        let cycle_id = e
            .get("cycle_id")
            .and_then(Value::as_str)
            .unwrap_or("cycle-000001")
            .to_string();
        let at = e.get("at").and_then(Value::as_i64).unwrap_or(NOW);
        let intent_id = e
            .get("intent_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| format!("int-{}", &key[..key.len().min(16)]));
        let details = e.get("details").cloned().unwrap_or_else(|| json!({}));
        ledger
            .append(
                event_type, &intent_id, key, &cycle_id, at, &amounts, &details,
            )
            .unwrap_or_else(|err| panic!("append {event_type} for {key}: {err}"));
    }
    ledger.replay().expect("replay succeeds")
}

#[test]
fn scenario_25_missing_execution_cost() {
    let case = case_json("25-missing-execution-cost");
    let events = case["inputs"]["events"]
        .as_array()
        .expect("inputs.events array");
    let state = replay_ledger_events(events);
    let produced = json!({"anomalies": state.anomalies, "spent_msat": state.spent_msat});
    assert_canon_eq(&produced, &case["expected"], "25-missing-execution-cost");
}

#[test]
fn scenario_26_unknown_execution_outcome() {
    let case = case_json("26-unknown-execution-outcome");
    let lifecycle = case["inputs"]["lifecycle"]
        .as_array()
        .expect("inputs.lifecycle array");

    // The wire shape here is a bare list of event-type strings with no
    // idempotency_key of its own (unlike scenario 25's full events);
    // mirrors the generator's hardcoded `intent_id="i-26"`,
    // `idempotency_key="k-26"`, `cycle_id="c-26"`, and `budget_reserved`'s
    // `reserved_msat: 5_000` amount (`tools/conformance/
    // generate_scenarios.py::s26`).
    let dir = TempDir::new().unwrap();
    let ledger = EconLedger::open(dir.path().join("l.db")).unwrap();
    let key = "k-26";
    for event_type in lifecycle {
        let et = event_type.as_str().expect("lifecycle entries are strings");
        let amounts: BTreeMap<String, i64> = if et == "budget_reserved" {
            [("reserved_msat".to_string(), 5_000i64)]
                .into_iter()
                .collect()
        } else {
            BTreeMap::new()
        };
        ledger
            .append(et, "i-26", key, "c-26", NOW, &amounts, &json!({}))
            .unwrap_or_else(|e| panic!("append {et}: {e}"));
    }
    let state = ledger.replay().unwrap();
    let produced = json!({"terminal": state.terminal, "reserved_msat": state.reserved_msat});
    assert_canon_eq(&produced, &case["expected"], "26-unknown-execution-outcome");
}

#[test]
fn scenario_40_sanitized_production_decisions() {
    let case = case_json("40-sanitized-production-decisions");
    let events = case["inputs"]["events"]
        .as_array()
        .expect("inputs.events array");
    let state = replay_ledger_events(events);
    let produced = json!({"replay": {
        "reserved_msat": state.reserved_msat,
        "spent_msat": state.spent_msat,
        "terminal": state.terminal,
        "anomalies": state.anomalies,
    }});
    assert_canon_eq(
        &produced,
        &case["expected"],
        "40-sanitized-production-decisions (case.json)",
    );

    // Additionally: replay `expected-ledger-events.json`'s events and
    // compare the projection against `expected-projections.json`.
    let ledger_events_case = scenario_file(
        "40-sanitized-production-decisions",
        "expected-ledger-events.json",
    );
    let events2 = ledger_events_case["expected"]["events"]
        .as_array()
        .expect("expected.events array");
    let state2 = replay_ledger_events(events2);
    let projection = json!({
        "schema_name": "ledger_projection",
        "schema_version": 0,
        "reserved_msat": state2.reserved_msat,
        "spent_msat": state2.spent_msat,
        "total_spent_msat": state2.total_spent_msat,
        "terminal": state2.terminal,
        "anomalies": state2.anomalies,
    });
    let expected_projections = scenario_file(
        "40-sanitized-production-decisions",
        "expected-projections.json",
    );
    assert_canon_eq(
        &projection,
        &expected_projections,
        "40-sanitized-production-decisions (expected-projections.json)",
    );
}

// --- failure_mode (27) ---

#[test]
fn scenario_27_boltz_timeout_after_acceptance() {
    let case = case_json("27-boltz-timeout-after-acceptance");
    let inputs = &case["inputs"];
    let age_seconds = inputs["age_seconds"].as_i64().expect("age_seconds int");
    let stale_after_seconds = inputs["stale_after_seconds"]
        .as_i64()
        .expect("stale_after_seconds int");
    let lifecycle = inputs["lifecycle"]
        .as_array()
        .expect("inputs.lifecycle array");

    let dir = TempDir::new().unwrap();
    let ledger = EconLedger::open(dir.path().join("l.db")).unwrap();
    let key = "k-27";
    for event_type in lifecycle {
        let et = event_type.as_str().expect("lifecycle entries are strings");
        ledger
            .append(
                et,
                "i-27",
                key,
                "c-27",
                NOW - age_seconds,
                &BTreeMap::new(),
                &json!({}),
            )
            .unwrap_or_else(|e| panic!("append {et}: {e}"));
    }

    let db_states: BTreeMap<String, DbReservationState> = BTreeMap::new();
    let report =
        reconcile(&ledger, &db_states, NOW, stale_after_seconds).expect("reconcile succeeds");
    let resolvable_as_db_missing = report.divergences.iter().any(|d| d.kind == "db_missing");
    let quarantine_when_stale = report
        .divergences
        .iter()
        .any(|d| d.kind == "unknown_outcome" && d.resolution.is_none());
    let produced = json!({
        "resolvable_as_db_missing": resolvable_as_db_missing,
        "quarantine_when_stale": quarantine_when_stale,
    });
    assert_canon_eq(
        &produced,
        &case["expected"],
        "27-boltz-timeout-after-acceptance",
    );
}

// --- intent_semantics (32, 33, 34) ---

#[test]
fn scenario_32_numeric_overflow_underflow() {
    let case = case_json("32-numeric-overflow-underflow");
    let overflow = match Msat::from_checked(1i128 << 63) {
        Ok(_) => "accepted",
        Err(_) => "EconArithmeticError",
    };
    let negative = match Msat::from_checked(-1i128) {
        Ok(_) => "accepted",
        Err(_) => "EconArithmeticError",
    };
    let produced = json!({"overflow_2_pow_63": overflow, "negative_msat": negative});
    assert_canon_eq(
        &produced,
        &case["expected"],
        "32-numeric-overflow-underflow",
    );
}

#[test]
fn scenario_33_msat_rounding_boundaries() {
    let case = case_json("33-msat-rounding-boundaries");
    let msat = case["inputs"]["msat"]
        .as_array()
        .expect("inputs.msat array");
    let floor_sats: Vec<i64> = msat
        .iter()
        .map(|v| {
            Msat::new(v.as_i64().unwrap())
                .unwrap()
                .to_sats_floor()
                .value()
        })
        .collect();
    let ceil_sats: Vec<i64> = msat
        .iter()
        .map(|v| {
            Msat::new(v.as_i64().unwrap())
                .unwrap()
                .to_sats_ceil()
                .value()
        })
        .collect();
    let produced = json!({"floor_sats": floor_sats, "ceil_sats": ceil_sats});
    assert_canon_eq(&produced, &case["expected"], "33-msat-rounding-boundaries");
}

#[test]
fn scenario_34_expired_intent() {
    let case = case_json("34-expired-intent");
    let inputs = &case["inputs"];
    let env = from_wire(&inputs["intent"]).expect("intent wire parses");
    let probes = inputs["probe_times"].as_array().expect("probe_times array");
    let results: Vec<bool> = probes
        .iter()
        .map(|t| is_expired(&env, UnixTime::new(t.as_i64().unwrap()).unwrap()))
        .collect();
    let produced = json!({"is_expired": results});
    assert_canon_eq(&produced, &case["expected"], "34-expired-intent");
}

// --- determinism (36, 37) ---

#[test]
fn scenario_36_map_order_independence() {
    let case = case_json("36-map-order-independence");
    // Verbatim from `tools/conformance/generate_scenarios.py::s36`: two
    // differently-ordered (including nested-object order) literals with
    // identical content.
    let val_a: Value = serde_json::from_str(r#"{"b":2,"a":1,"nested":{"y":2,"x":1}}"#).unwrap();
    let val_b: Value = serde_json::from_str(r#"{"nested":{"x":1,"y":2},"a":1,"b":2}"#).unwrap();
    let canon_a = canonical_json(&val_a).unwrap();
    let canon_b = canonical_json(&val_b).unwrap();
    let produced = json!({
        "canonical": canon_a,
        "insertion_order_independent": canon_a == canon_b,
    });
    assert_canon_eq(&produced, &case["expected"], "36-map-order-independence");
}

fn default_rebalance_fields() -> IntentFields {
    let created_at = UnixTime::new(NOW).unwrap();
    IntentFields {
        intent_type: "REBALANCE".to_string(),
        snapshot_id: "snap-1".to_string(),
        created_at,
        expires_at: created_at.plus_seconds(600).unwrap(),
        target: "111x222x0".to_string(),
        amount_msat: Some(Msat::new(400_000_000).unwrap()),
        expected_benefit_msat: SignedMsat(0),
        max_cost_msat: Msat::new(3_000_000).unwrap(),
        capital_committed_msat: Msat::new(400_000_000).unwrap(),
        confidence_micro: Micro::new(0).unwrap(),
        reason_codes: vec![],
        explanation: Explanation {
            kind: "conformance".to_string(),
            components: vec![("case".to_string(), json!(1))],
        },
        preconditions: vec![],
        priority: 50,
        budget_bucket: "rebalance".to_string(),
        origin_policy: "conformance".to_string(),
        reversible: false,
    }
}

#[test]
fn scenario_37_clock_seed_determinism() {
    let case = case_json("37-clock-seed-determinism");
    let env1 = make_intent(default_rebalance_fields()).unwrap();
    let env2 = make_intent(default_rebalance_fields()).unwrap();
    let produced = json!({
        "intent_ids_equal": env1.intent_id == env2.intent_id,
        "idempotency_keys_equal": env1.idempotency_key == env2.idempotency_key,
        "intent_id": env1.intent_id.as_str(),
    });
    assert_canon_eq(&produced, &case["expected"], "37-clock-seed-determinism");
}
