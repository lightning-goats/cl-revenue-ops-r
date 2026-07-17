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
//! 37 econ-core/analytics/budget-rail/fee-controller/rebalance-mode
//! scenarios are replayed byte-identically (Step 2; Phase 3 Task 10 added 9
//! to the original Phase 2 Task 9 count of 18, Phase 4 Task 12 added 6 more
//! — the fee-stage rails/DTS-PID scenarios 08-12 plus the admission
//! scenario 13, all replayed through the real merged `revops-fees`
//! modules — and Phase 5 Task 9 added 4 more — scenarios 03/14/15/17,
//! replayed through the real merged `revops-rebalance` planner and MODES
//! table); the other 3 are schema-gated only, via the pinned,
//! test-enforced [`DEFERRED`] skip list (Step 3) — every scenario
//! directory must be either replayed or deferred, so adding scenario 41
//! breaks the build until triaged. Scenario 16 (structural-drain) is NOT a
//! clean 40/2 corpus closure: its `expected` is pure Boltz capital-stack
//! output (Phase 6) with no Phase-5-owned planner slice to replay — see
//! [`DEFERRED`]'s note. This is a deliberate, logged exception to the
//! phase plan's target count, not an oversight.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use revops_analytics::classification::{revenue_role_30d, ChannelRole};
use revops_analytics::policy::FeeStrategy;
use revops_analytics::profitability::{
    classify_channel, ChannelCosts, ChannelProfitability, ChannelRevenue, ClassifyEvidence,
    DiagStats, ProfitabilityClass,
};
use revops_analytics::protection::{
    close_protection_reason, policy_close_block, FlowEvidence, ProtProfEvidence,
};
use revops_core::canonical::canonical_json;
use revops_db::budget::{BudgetDb, ReserveRequest};
use revops_econ::arbiter::{arbitrate, ArbitrationResult};
use revops_econ::governor::{authority_allows, GovernorDecision, GovernorFacade};
use revops_econ::intents::{
    from_wire, is_expired, make_intent, Explanation, IntentEnvelope, IntentFields,
};
use revops_econ::ledger::{EconLedger, LedgerState};
use revops_econ::pyfloat::{py_repr, py_round};
use revops_econ::reconcile::{reconcile, DbReservationState};
use revops_econ::types::{EconResult, Micro, Msat, SignedMsat, UnixTime};
use revops_fees::admission::{compute_htlcmax_msat, HtlcmaxCfg};
use revops_fees::floors::calculate_floor;
use revops_fees::pid::{calculate_multiplier, PidState};
use revops_fees::profiles::FeeProfileSettings;
use revops_fees::rails::apply_damped_fee_target;
use revops_fees::thompson::dynamics::update_posterior;
use revops_fees::thompson::GaussianThompsonState;
use revops_rebalance::ev::{sats_ev_gate, EvInputs};
use revops_rebalance::modes::mode;
use revops_rebalance::planner::{plan, PlannedPair, PlannerChannel};
use revops_rebalance::types::{DrainDemand, SkipRecord};
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

/// The 37 econ-core/analytics/budget-rail/fee-controller/rebalance-mode
/// scenarios replayed byte-identically in this file: 18 from Phase 2 Task
/// 9, 9 flipped in Phase 3 Task 10, 6 flipped in Phase 4 Task 12, and 4
/// flipped in Phase 5 Task 9 (scenarios 03/14/15/17, via the real merged
/// `revops-rebalance` planner and MODES table — 16 stays deferred, see
/// [`DEFERRED`]'s note).
const REPLAYED: &[&str] = &[
    "01-ordinary-profitable-channel",
    "02-source-gateway-protection",
    "03-sink-depletion",
    "04-balanced-channel",
    "05-underwater-classification",
    "06-stagnant-candidate",
    "07-zombie-classification",
    "08-fee-rail",
    "09-fee-rate-limit",
    "10-fee-deadband",
    "11-fee-cooldown",
    "12-dts-pid-components",
    "13-dynamic-htlcmax",
    "14-hot-channel-priority",
    "15-normal-rebalance",
    "17-manual-diagnostic-rebalance",
    "18-conflicting-close-rebalance",
    "19-protected-close-rejection",
    "20-open-vs-lnplus",
    "21-circular-vs-boltz-structural",
    "22-budget-exhaustion",
    "23-concurrent-reservation-contention",
    "24-restart-outstanding-reservation",
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

/// Explicit, never-silent skip list: the 3 scenarios owned by later phases
/// (or with no code path at all). Adding scenario 41 without triaging it
/// into either this list or [`REPLAYED`] fails
/// [`all_scenarios_replayed_or_deferred`].
///
/// Phase 3 Task 10 re-tag (deliberate, logged per the conflict-rules-flip
/// convention): scenarios 08-12's owner strings said "Phase 3: fee_stage
/// controller" — that numbering predated the port phase plan. Re-tagged to
/// "Phase 4: fee controller (rails/rate-limit/deadband/cooldown/DTS-PID)".
/// Scenario 19 ("Phase 3-5: close-protection gate") left this list
/// entirely — the pure gate (`policy_close_block`) now replays; the *live*
/// close-protection golden suite still rides Phase 6 capacity work.
/// Scenario 39 stays deferred (prose-only, no code path).
///
/// Phase 4 Task 12 ownership audit: 08-12 (`fee_stage`
/// rails/rate_limit/deadband/cooldown/DTS-PID) and 13 (`admission`
/// dynamic-htlcmax) are genuinely phase-4-owned — every function they
/// replay (`revops_fees::floors::calculate_floor`,
/// `revops_fees::rails::apply_damped_fee_target`,
/// `revops_fees::thompson::dynamics::update_posterior`,
/// `revops_fees::pid::calculate_multiplier`,
/// `revops_fees::admission::compute_htlcmax_msat`) is implemented by this
/// plan, so they moved to [`REPLAYED`].
///
/// Phase 5 Task 9 flip (this flip, gate task): 03/14/15/17
/// (`rebalance_mode` planner + MODES table) were RELABELED in Phase 4 Task
/// 12 to "Phase 5: rebalance stack" without being replayed — this task
/// closes that out for real, moving all four to [`REPLAYED`] via
/// `revops_rebalance::planner::plan` (03, 15) and
/// `revops_rebalance::modes::mode` (14, 17). 16 (structural-drain) does
/// NOT move: its `expected` is pure Boltz capital-stack output (Phase 6)
/// with no Phase-5-owned `drain_demand` residual present to replay against
/// — see its entry below for the full provenance trace. This is why the
/// corpus lands at 37 replayed / 3 deferred rather than the plan's target
/// 38/2 (logged honestly, not silently fudged — Task 9 brief's
/// gate-honesty rule).
const DEFERRED: &[(&str, &str)] = &[
    (
        "16-structural-drain",
        "Phase 6: Boltz loop-out execution (capital stack) — this scenario's \
         `expected` is a pure Boltz cycle_dry_run plan/execution wire \
         (executed/plan/recommendations/budget), sourced from \
         tests/golden/fixtures/boltz/cycle_dry_run_executable_balance_plan.json; \
         it carries NO `drain_demand` key, so there is no Phase-5-owned \
         planner slice in THIS scenario's expectations to replay against \
         T2's PlanOutput — deferring the whole case, not a silent partial \
         pass (gate-honesty rule, Task 9 brief).",
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
    assert_eq!(REPLAYED.len(), 37, "expected exactly 37 replayed scenarios");
    assert_eq!(DEFERRED.len(), 3, "expected exactly 3 deferred scenarios");

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

// --- classification (01, 02, 04, 05, 06, 07) ---

fn channel_role_from_name(name: &str) -> ChannelRole {
    match name {
        "INBOUND_GATEWAY" => ChannelRole::InboundGateway,
        "OUTBOUND_GATEWAY" => ChannelRole::OutboundGateway,
        "BALANCED" => ChannelRole::Balanced,
        "DORMANT" => ChannelRole::Dormant,
        other => panic!("unknown ChannelRole name: {other}"),
    }
}

fn default_classify_evidence(now: i64) -> ClassifyEvidence<'static> {
    ClassifyEvidence {
        now,
        diag_stats: None,
        posterior_variance: None,
        contribution_30d_msat: None,
    }
}

#[test]
fn scenario_01_ordinary_profitable_channel() {
    let case = case_json("01-ordinary-profitable-channel");
    let inputs = &case["inputs"];
    let result = classify_channel(
        inputs["roi"].as_f64().expect("roi float"),
        inputs["net_profit"].as_i64().expect("net_profit int"),
        inputs["last_routed"].as_i64(),
        inputs["days_open"].as_i64().expect("days_open int"),
        inputs["forward_count"].as_i64().expect("forward_count int"),
        &default_classify_evidence(NOW),
    );
    let produced = json!({"classification": result.as_name()});
    assert_canon_eq(
        &produced,
        &case["expected"],
        "01-ordinary-profitable-channel",
    );
}

#[test]
fn scenario_02_source_gateway_protection() {
    let case = case_json("02-source-gateway-protection");
    // Recipe pinned by the task brief (this fixture's `inputs` is `{}` — the
    // evidence is not itself vendored in the corpus): role_30d =
    // INBOUND_GATEWAY, marginal_roi_percent = -10.0, flow(confidence=0.9,
    // forward_count=50), empty revenue-route set. Mirrors
    // `tests/protection.rs::golden_gateway_30d_protected`'s `prof(...)`.
    let prof = ProtProfEvidence {
        role_30d: Some(ChannelRole::InboundGateway),
        lifetime_role: ChannelRole::Balanced,
        marginal_roi_percent: -10.0,
        window_30d_available: true,
        sourced_fee_30d_msat: 0,
        lifetime_sourced_fee_sats: 0,
        days_open: 100,
    };
    let flow = FlowEvidence {
        confidence: Some(0.9),
        forward_count: Some(50),
    };
    let reason = close_protection_reason("111x222x0", &prof, Some(&flow), &BTreeSet::new(), 7);
    let produced = json!({"reason": reason});
    assert_canon_eq(&produced, &case["expected"], "02-source-gateway-protection");
}

#[test]
fn scenario_04_balanced_channel() {
    let case = case_json("04-balanced-channel");
    let inputs = &case["inputs"];
    let lifetime_role = channel_role_from_name(
        inputs["lifetime_role"]
            .as_str()
            .expect("lifetime_role string"),
    );
    let result = revenue_role_30d(
        inputs["window_30d_available"]
            .as_bool()
            .expect("window_30d_available bool"),
        inputs["forward_count_30d"]
            .as_i64()
            .expect("forward_count_30d int"),
        inputs["sourced_forward_count_30d"]
            .as_i64()
            .expect("sourced_forward_count_30d int"),
        lifetime_role,
    );
    let produced = json!({"role_30d": result.as_name()});
    assert_canon_eq(&produced, &case["expected"], "04-balanced-channel");
}

/// Minimal [`ChannelProfitability`] carrying only the two fields
/// [`ChannelProfitability::marginal_roi`] actually reads
/// (`marginal_profit_30d_sats`, `rebalance_cost_30d_sats`); every other
/// field is inert filler (mirrors `tests/profitability.rs::prof_with_marginal`
/// in the revops-analytics crate, which this replay cannot import — it's a
/// `#[cfg(test)]`-private helper of a different crate's test binary).
fn profitability_for_marginal_roi(profit_30d: i64, cost_30d: i64) -> ChannelProfitability {
    let costs = ChannelCosts {
        channel_id: "111x222x0".to_string(),
        peer_id: format!("02{}", "a".repeat(64)),
        open_cost_sats: 0,
        rebalance_cost_sats: 0,
        effective_rebalance_cost_sats: 0,
    };
    let revenue = ChannelRevenue {
        channel_id: "111x222x0".to_string(),
        fees_earned_msat: 0,
        volume_routed_msat: 0,
        forward_count: 0,
        sourced_volume_msat: 0,
        sourced_fee_contribution_msat: 0,
        sourced_forward_count: 0,
    };
    ChannelProfitability {
        channel_id: "111x222x0".to_string(),
        peer_id: format!("02{}", "a".repeat(64)),
        capacity_sats: 0,
        costs,
        revenue,
        net_profit_sats: 0,
        roi_percent: 0.0,
        classification: ProfitabilityClass::BreakEven,
        cost_per_sat_routed: 0.0,
        fee_per_sat_routed: 0.0,
        days_open: 0,
        last_routed: None,
        marginal_profit_30d_sats: profit_30d,
        rebalance_cost_30d_sats: cost_30d,
        opener: "local".to_string(),
        contribution_30d_msat: 0,
        fees_earned_30d_msat: 0,
        sourced_fee_30d_msat: 0,
        forward_count_30d: 0,
        sourced_forward_count_30d: 0,
        window_30d_available: false,
    }
}

/// Documented exception (task brief): the fixture's `expected.marginal_roi`
/// is a JSON FLOAT (`-0.5`), and `canonical_json` categorically FORBIDS
/// non-integer numbers (`revops_core::canonical::CanonicalError::
/// NonIntegerNumber`) — so this scenario cannot go through
/// [`assert_canon_eq`] at all, unlike every other replay in this file.
/// Comparison is `f64::to_bits` bitwise identity against the fixture's own
/// parsed float, per the brief's pinned choice (over a py_repr-string
/// round-trip) — not epsilon-fuzzed, and -0.5 has an exact `f64`
/// representation on both sides so the two forms agree here regardless.
#[test]
fn scenario_05_underwater_classification() {
    let case = case_json("05-underwater-classification");
    let inputs = &case["inputs"];
    let profitability = profitability_for_marginal_roi(
        inputs["marginal_profit_30d_sats"]
            .as_i64()
            .expect("marginal_profit_30d_sats int"),
        inputs["rebalance_cost_30d_sats"]
            .as_i64()
            .expect("rebalance_cost_30d_sats int"),
    );
    let produced = profitability.marginal_roi();
    let expected = case["expected"]["marginal_roi"]
        .as_f64()
        .expect("expected.marginal_roi is a JSON float");
    assert_eq!(
        produced.to_bits(),
        expected.to_bits(),
        "05-underwater-classification: marginal_roi bit-pattern mismatch (produced {produced}, expected {expected})"
    );
}

#[test]
fn scenario_06_stagnant_candidate() {
    let case = case_json("06-stagnant-candidate");
    let inputs = &case["inputs"];
    let result = classify_channel(
        inputs["roi"].as_f64().expect("roi float"),
        inputs["net_profit"].as_i64().expect("net_profit int"),
        inputs["last_routed"].as_i64(),
        inputs["days_open"].as_i64().expect("days_open int"),
        inputs["forward_count"].as_i64().expect("forward_count int"),
        &default_classify_evidence(NOW),
    );
    let produced = json!({"classification": result.as_name()});
    assert_canon_eq(&produced, &case["expected"], "06-stagnant-candidate");
}

#[test]
fn scenario_07_zombie_classification() {
    let case = case_json("07-zombie-classification");
    // Recipe pinned by the task brief (this fixture's `inputs` is `{}`):
    // DiagStats{attempt_count: 2, last_success_time: 0}, roi -0.40,
    // last_routed = NOW - 30 days, days_open 200, forward_count 5.
    let diag = DiagStats {
        attempt_count: 2,
        last_success_time: 0,
    };
    let ev = ClassifyEvidence {
        now: NOW,
        diag_stats: Some(&diag),
        posterior_variance: None,
        contribution_30d_msat: None,
    };
    let result = classify_channel(-0.40, 0, Some(NOW - 86_400 * 30), 200, 5, &ev);
    let produced = json!({"classification": result.as_name()});
    assert_canon_eq(&produced, &case["expected"], "07-zombie-classification");
}

// --- fee controller (08, 09, 10, 11, 12) + admission (13) ---
//
// Phase 4 Task 12 flip: these six scenarios replay through the REAL merged
// `revops-fees` modules (ADR-001 rail stages + the DTS/PID controller +
// the htlcmax admission valve) — not reimplementations.

#[test]
fn scenario_08_fee_rail_floor() {
    let case = case_json("08-fee-rail");
    let inputs = &case["inputs"];
    let capacity_sats = inputs["capacity_sats"].as_i64().expect("capacity_sats int");
    let opener = inputs["opener"].as_str().expect("opener string");
    assert!(
        inputs["chain_costs"].is_null(),
        "08-fee-rail: this replay only covers the chain_costs=None input this fixture pins"
    );

    let floor_ppm = calculate_floor(capacity_sats, None, None, opener);
    let produced = json!({"floor_ppm": floor_ppm});
    assert_canon_eq(&produced, &case["expected"], "08-fee-rail");
}

/// The golden damping suite (`fixtures/golden/fee/damping_*.json`, replayed
/// by `revops-fees`'s own `tests/rails.rs::golden_damping_scenarios_replay_exactly`)
/// pins a CUSTOM profile — not either named [`FeeProfileSettings`] table —
/// per `test_golden_fee_damping.py::PROFILE`: `wake_cycle_max_delta_ratio
/// =0.50, normal_cycle_max_delta_ratio=0.15, wake_cycle_min_delta_ppm=25,
/// normal_cycle_min_delta_ppm=10`. Conformance scenarios 09/10/11 are
/// sourced from those exact same vendored golden files (see each case's
/// `source` field) and their `expected` values match the goldens
/// byte-for-byte, so this replay uses the identical profile — NOT
/// `fee_profile("active")`, which produces different (wrong) capped deltas
/// for these inputs. The other profile fields are irrelevant to
/// `apply_damped_fee_target` (it only reads the four delta-cap fields), so
/// they are zeroed here rather than guessing at unrelated values.
const GOLDEN_DAMPING_PROFILE: FeeProfileSettings = FeeProfileSettings {
    min_observation_hours: 0.0,
    min_forwards_for_signal: 0,
    dts_discount_gamma: 0.0,
    dts_sparse_discount_gamma: 0.0,
    normal_target_blend_ratio: 0.0,
    wake_target_blend_ratio: 0.0,
    sparse_target_blend_ratio: 0.0,
    normal_cycle_max_delta_ratio: 0.15,
    normal_cycle_min_delta_ppm: 10,
    wake_cycle_max_delta_ratio: 0.50,
    wake_cycle_min_delta_ppm: 25,
};

fn run_damping_scenario(dir: &str) {
    let case = case_json(dir);
    let inputs = &case["inputs"];
    let current = inputs["current"].as_i64().expect("current int");
    let target = inputs["target"].as_i64().expect("target int");
    let woke = inputs["woke"].as_bool().expect("woke bool");

    let (applied_fee_ppm, diag) =
        apply_damped_fee_target(current, target, woke, &GOLDEN_DAMPING_PROFILE);
    let produced = json!({
        "applied_fee_ppm": applied_fee_ppm,
        "diag": {
            "cap_applied": diag.cap_applied,
            "cap_reason": diag.cap_reason,
            "max_delta_ppm": diag.max_delta_ppm,
            "requested_delta_ppm": diag.requested_delta_ppm,
            "wake_damping_applied": diag.wake_damping_applied,
        },
    });
    assert_canon_eq(&produced, &case["expected"], dir);
}

#[test]
fn scenario_09_fee_rate_limit_clamp() {
    run_damping_scenario("09-fee-rate-limit");
}

#[test]
fn scenario_10_fee_deadband_no_change() {
    run_damping_scenario("10-fee-deadband");
}

#[test]
fn scenario_11_fee_cooldown_wake_cycle() {
    run_damping_scenario("11-fee-cooldown");
}

/// Documented exception (like scenario 05): `expected` is a JSON object of
/// FLOAT leaves (`pid_ewma_error`/`pid_multiplier`/`posterior_mean`/
/// `posterior_std`), and `canonical_json` categorically forbids
/// non-integer numbers — so this scenario cannot go through
/// [`assert_canon_eq`]. The generator (`tools/conformance/
/// generate_scenarios.py:226-229`) computed every expected float as
/// `round(x, 12)` before embedding it; comparison here is
/// `py_repr(py_round(actual, 12)) == py_repr(expected_as_f64)` per field —
/// still bit-for-bit on the rounded decimal string, no epsilon.
#[test]
fn scenario_12_dts_pid_components() {
    let case = case_json("12-dts-pid-components");
    let inputs = &case["inputs"];
    let pid_inputs = &inputs["pid"];
    let observations = inputs["dts_observations"]
        .as_array()
        .expect("dts_observations array");
    assert_eq!(
        pid_inputs["fresh_state"].as_bool(),
        Some(true),
        "12-dts-pid-components: this replay only covers the fresh_state=true case this fixture pins"
    );

    // Fresh PidState::default() already has last_update_time=0, which the
    // ported calculate_multiplier treats as "dt=0" (py: `pid.last_update_time
    // = -1` forces the same dt=0 branch) — no special-casing needed here.
    let mut pid_state = PidState::default();
    let pid_multiplier = calculate_multiplier(
        &mut pid_state,
        pid_inputs["current_outbound_ratio"]
            .as_f64()
            .expect("current_outbound_ratio float"),
        pid_inputs["capacity_sats"]
            .as_i64()
            .expect("capacity_sats int"),
        pid_inputs["flow_state"]
            .as_str()
            .expect("flow_state string"),
        NOW,
    );

    let mut ts_state = GaussianThompsonState::default();
    for obs in observations {
        update_posterior(
            &mut ts_state,
            obs["fee"].as_f64().expect("fee float"),
            obs["revenue_rate"].as_f64().expect("revenue_rate float"),
            obs["hours"].as_f64().expect("hours float"),
            "normal",
            false,
            NOW,
        );
    }

    let checks: &[(&str, f64)] = &[
        ("pid_ewma_error", pid_state.ewma_error),
        ("pid_multiplier", pid_multiplier),
        ("posterior_mean", ts_state.posterior_mean),
        ("posterior_std", ts_state.posterior_std),
    ];
    for (field, actual) in checks {
        let expected = case["expected"][field]
            .as_f64()
            .unwrap_or_else(|| panic!("12-dts-pid-components: expected.{field} is a JSON float"));
        let produced_repr = py_repr(py_round(*actual, 12));
        let expected_repr = py_repr(expected);
        assert_eq!(
            produced_repr, expected_repr,
            "12-dts-pid-components: {field} mismatch (produced {produced_repr}, expected {expected_repr})"
        );
    }
}

#[test]
fn scenario_13_dynamic_htlcmax() {
    let case = case_json("13-dynamic-htlcmax");
    let inputs = &case["inputs"];
    let channel_info = &inputs["channel_info"];
    let capacity_sats = channel_info["capacity"].as_i64().expect("capacity int");
    let spendable_msat = channel_info["spendable_msat"]
        .as_i64()
        .expect("spendable_msat int");
    let flow_state = inputs["flow_state"].as_str().expect("flow_state string");
    assert!(
        inputs["cfg_overrides"]
            .as_object()
            .expect("cfg_overrides object")
            .is_empty(),
        "13-dynamic-htlcmax: this replay only covers the empty-cfg_overrides case this fixture pins"
    );

    // `test_golden_htlcmax.py::_cfg` defaults (no overrides in this
    // fixture's inputs) — same defaults `revops-fees`'s own golden
    // admission suite pins.
    let cfg = HtlcmaxCfg {
        enable_dynamic_htlcmax: Value::Bool(true),
        htlcmax_source_pct: 0.85,
        htlcmax_sink_pct: 0.25,
        htlcmax_balanced_pct: 0.50,
    };

    let htlcmax_msat = compute_htlcmax_msat(&cfg, capacity_sats, spendable_msat, flow_state)
        .expect("htlcmax valve enabled and capacity > 0");
    let produced = json!({"htlcmax_msat": htlcmax_msat});
    assert_canon_eq(&produced, &case["expected"], "13-dynamic-htlcmax");
}

// --- rebalance mode (03, 14, 15, 17) ---
//
// Phase 5 Task 9 (conformance un-defer flip): these four scenarios replay
// through the REAL merged `revops-rebalance` modules — the planner
// (`revops_rebalance::planner::plan`) and the MODES table
// (`revops_rebalance::modes::mode`) — not reimplementations. 16 stays
// deferred (see DEFERRED's note): its `expected` is pure Boltz
// execution/plan output with no Phase-5-owned `drain_demand` slice.

/// Direct recipe port of `tests/golden/test_golden_rebalance_planner.py`'s
/// `_ch(cid, local_ratio, **over)` helper (real-Python `ChannelState`
/// defaults this replay does not vary): `capacity_sats=2_000_000`,
/// `actual_inbound_fee_ppm=100`, `local_out_fee_ppm=250`, `is_active=True`
/// (feeds `dest_fee_history_validated`), `remaining_budget_sats=5_000`,
/// and every other `ChannelState` field at ITS dataclass default
/// (`rebalance_state_v2.py`: `dest_urgency=source_drain_score=0.0`,
/// `activity_out_sats=activity_in_sats=0`,
/// `historical_direct_fee_ppm=historical_sourced_fee_ppm=0.0`,
/// `budget_source="none"`, `realized_utilization=0.5`,
/// `utilization_is_realized=False`). `target_band_low=0.35`/
/// `target_band_high=0.65` are `RebalancePlanner()`'s own constructor
/// defaults (both golden cases construct the planner with no override).
struct PlannerRecipeChannel {
    id: &'static str,
    local_ratio: f64,
    value_class: &'static str,
    capacity_sats: i64,
    remaining_budget_sats: i64,
}

fn recipe_planner_channel(r: &PlannerRecipeChannel) -> PlannerChannel {
    let first = r.id.chars().next().expect("non-empty channel id");
    PlannerChannel {
        channel_id: r.id.to_string(),
        peer_id: format!("02{}", first.to_string().repeat(64)),
        capacity_sats: r.capacity_sats,
        spendable_sats: py_round(r.local_ratio * r.capacity_sats as f64, 0) as i64,
        receivable_sats: r.capacity_sats
            - py_round(r.local_ratio * r.capacity_sats as f64, 0) as i64,
        band_low: 0.35,
        band_high: 0.65,
        inbound_ppm: 100,
        value_class: r.value_class.to_string(),
        urgency: 0.0,
        drain: 0.0,
        capex_remaining_sats: r.remaining_budget_sats,
    }
}

/// Value-class score table (py `_VALUE_SCORES`, `rebalance_planner_v2.py`
/// ~23-28), reproduced here (not imported — it is a private `planner.rs`
/// helper) purely to reconstruct `score_decomposition.inputs.value_score`,
/// an echo field the planner's real `PairCandidate` dataclass carries that
/// T2's reduced `PlannedPair` does not expose.
fn recipe_value_score(value_class: &str) -> i64 {
    match value_class {
        "profitable" => 2,
        "active" | "funded" => 1,
        _ => 0,
    }
}

/// Direct port of `rebalance_planner_v2.py`'s `_bootstrap_score_decomposition`
/// (module-level pure function, NOT the engine's `_build_score_decomposition`
/// — this is the planner's own pre-route explainability annotation, stage
/// `"planner_pre_route"`). Every argument here is either a T2 `PlannedPair`
/// output (`amount_sats`/`pair_budget_sats`/`score`) or directly derivable
/// from the SAME recipe inputs `plan()` consumed (value_score/imbalance_score/
/// the four additive terms/local ratios) — no planner internals are
/// reimplemented, only this genuinely-separate annotation function.
#[allow(clippy::too_many_arguments)]
fn bootstrap_score_decomposition(
    value_score: i64,
    imbalance_score: f64,
    pair_score: f64,
    amount_sats: i64,
    pair_budget_sats: i64,
    source_local_ratio: f64,
    dest_local_ratio: f64,
    dest_urgency_term: f64,
    source_drain_term: f64,
    dest_value_term: f64,
    cheap_return_term: f64,
) -> Value {
    let p_success = 0.5;
    let expected_future_value = py_round(pair_score, 6);
    let final_score = py_round(p_success * expected_future_value, 6);
    json!({
        "model_version": "v2-bootstrap-explainability",
        "score_units": "planner_score_minus_budget_share",
        "stage": "planner_pre_route",
        "p_success": p_success,
        "expected_future_value": expected_future_value,
        "expected_fee": 0.0,
        "source_opportunity_cost": 0.0,
        "failure_penalty": 0.0,
        "capital_risk_penalty": 0.0,
        "do_nothing_score": 0.0,
        "final_score": final_score,
        "beats_do_nothing": final_score > 0.0,
        "rejection_reason": "",
        "inputs": {
            "value_score": value_score,
            "imbalance_score": py_round(imbalance_score, 6),
            "amount_sats": amount_sats,
            "pair_budget_sats": pair_budget_sats,
            "source_local_ratio": py_round(source_local_ratio, 6),
            "dest_local_ratio": py_round(dest_local_ratio, 6),
            "dest_urgency_term": py_round(dest_urgency_term, 6),
            "source_drain_term": py_round(source_drain_term, 6),
            "dest_value_term": py_round(dest_value_term, 6),
            "cheap_return_term": py_round(cheap_return_term, 6),
        },
    })
}

/// Assembles the full `PairCandidate` wire shape for one selected pair —
/// T2's `PlannedPair` output plus the recipe's known `ChannelState` echo
/// fields (source/dest capacity, value class, out fee ppm, and every
/// zero/default-valued descriptive field the reduced `PlannerChannel` does
/// not carry — all constants at THIS recipe's inputs, see
/// `PlannerRecipeChannel`'s doc comment) plus the `score_decomposition`
/// annotation and the pre-route dataclass-default fields
/// (`reason_code="ev_positive"`, `rejection_reason=""`, `route=None`,
/// `route_cost_sats=None`, `route_decision=None` —
/// `rebalance_types_v2.py`'s `PairCandidate` field defaults).
#[allow(clippy::too_many_arguments)]
fn selected_pair_wire(
    src: &PlannerRecipeChannel,
    dest: &PlannerRecipeChannel,
    pair: &PlannedPair,
    dest_urgency_term: f64,
    source_drain_term: f64,
) -> Value {
    let src_ratio = src.local_ratio;
    let dest_ratio = dest.local_ratio;
    let dest_value_term = recipe_value_score(dest.value_class) as f64 * 0.20;
    let inbound_ppm_clamped = 100i64.clamp(0, 5_000);
    let cheap_return_term = ((5_000 - inbound_ppm_clamped) as f64 / 50_000.0).max(0.0);
    let src_imbalance = (src_ratio - 0.65_f64).max(0.0);
    let dest_imbalance = (0.35_f64 - dest_ratio).max(0.0);
    let imbalance_score = (src_imbalance + dest_imbalance) / 2.0;

    json!({
        "amount_sats": pair.amount_sats,
        "dest_activity_in_sats": 0,
        "dest_budget_source": "none",
        "dest_capacity_sats": dest.capacity_sats,
        "dest_channel_id": dest.id,
        "dest_fee_history_validated": true,
        "dest_historical_direct_fee_ppm": 0.0,
        "dest_historical_sourced_fee_ppm": 0.0,
        "dest_local_ratio": dest_ratio,
        "dest_out_fee_ppm": 250,
        "dest_peer_id": format!("02{}", dest.id.chars().next().unwrap().to_string().repeat(64)),
        "dest_realized_utilization": 0.5,
        "dest_utilization_is_realized": false,
        "dest_value_class": dest.value_class,
        "pair_budget_sats": pair.pair_budget_sats,
        "reason_code": "ev_positive",
        "rejection_reason": "",
        "route": null,
        "route_cost_sats": null,
        "route_decision": null,
        "score": pair.score,
        "score_decomposition": bootstrap_score_decomposition(
            recipe_value_score(dest.value_class),
            imbalance_score,
            pair.score,
            pair.amount_sats,
            pair.pair_budget_sats,
            src_ratio,
            dest_ratio,
            dest_urgency_term,
            source_drain_term,
            dest_value_term,
            cheap_return_term,
        ),
        "source_activity_out_sats": 0,
        "source_budget_source": "none",
        "source_capacity_sats": src.capacity_sats,
        "source_channel_id": src.id,
        "source_historical_direct_fee_ppm": 0.0,
        "source_historical_sourced_fee_ppm": 0.0,
        "source_local_ratio": src_ratio,
        "source_out_fee_ppm": 250,
        "source_peer_id": format!("02{}", src.id.chars().next().unwrap().to_string().repeat(64)),
        "source_realized_utilization": 0.5,
        "source_utilization_is_realized": false,
        "source_value_class": src.value_class,
    })
}

fn skip_record_wire(s: &SkipRecord) -> Value {
    json!({
        "channel_id": s.channel_id,
        "reason": s.reason,
        "value_class": s.value_class,
        "remaining_budget_sats": s.remaining_budget_sats,
        "detail": s.detail,
    })
}

/// `paired_count` (over-local channels successfully paired) and
/// `over_local_count` (all over-local channels, paired or residual) are
/// DERIVED from the actual `plan()` result, not hardcoded: every selected
/// pair's source is a distinct over-local channel in both recipes this
/// helper serves (each recipe has exactly one over-local channel), so
/// `paired_count == pairs_len` and `over_local_count == pairs_len +
/// entries.len()` (the residual `DrainDemand` entries `plan()` could not
/// place).
fn drain_demand_wire(entries: &[DrainDemand], pairs_len: usize) -> Value {
    json!({
        "entries": entries.iter().map(|e| json!({
            "channel_id": e.channel_id,
            "peer_id": e.peer_id,
            "excess_sats": e.excess_sats,
            "drain_score": e.drain_score,
            "value_class": e.value_class,
        })).collect::<Vec<_>>(),
        "total_excess_sats": entries.iter().map(|e| e.excess_sats).sum::<i64>(),
        "over_local_count": pairs_len + entries.len(),
        "paired_count": pairs_len,
    })
}

/// Deep structural equality that treats every JSON number the same way
/// `canonical_json` cannot here: `canonical_json` categorically forbids
/// non-integer numbers (`revops_core::canonical::CanonicalError::
/// NonIntegerNumber` — see scenario 05/12's doc comments), and scenarios
/// 03/15's `expected` is saturated with float leaves (scores, ratios, the
/// whole `score_decomposition` subtree). Integers compare by exact value;
/// floats compare via `py_repr` string equality (still bit-for-bit on the
/// rounded decimal string, never epsilon — Global Constraints: byte-parity
/// discipline); objects compare by exact key SET (not order — JSON object
/// order is not semantic) with recursive per-key comparison; arrays compare
/// element-wise in order (planner pair/skip lists are order-sensitive).
fn assert_deep_eq_pyfloat(produced: &Value, expected: &Value, path: &str) {
    match (produced, expected) {
        (Value::Number(p), Value::Number(e)) => {
            if p.is_i64() && e.is_i64() {
                assert_eq!(p.as_i64(), e.as_i64(), "{path}: integer mismatch");
            } else {
                let pf = p
                    .as_f64()
                    .unwrap_or_else(|| panic!("{path}: produced not numeric"));
                let ef = e
                    .as_f64()
                    .unwrap_or_else(|| panic!("{path}: expected not numeric"));
                assert_eq!(
                    py_repr(pf),
                    py_repr(ef),
                    "{path}: float mismatch (produced {pf}, expected {ef})"
                );
            }
        }
        (Value::Object(p), Value::Object(e)) => {
            let pk: BTreeSet<&String> = p.keys().collect();
            let ek: BTreeSet<&String> = e.keys().collect();
            assert_eq!(pk, ek, "{path}: object key set mismatch");
            for k in pk {
                assert_deep_eq_pyfloat(&p[k], &e[k], &format!("{path}.{k}"));
            }
        }
        (Value::Array(p), Value::Array(e)) => {
            assert_eq!(p.len(), e.len(), "{path}: array length mismatch");
            for (i, (pv, ev)) in p.iter().zip(e.iter()).enumerate() {
                assert_deep_eq_pyfloat(pv, ev, &format!("{path}[{i}]"));
            }
        }
        _ => assert_eq!(produced, expected, "{path}: mismatch"),
    }
}

#[test]
fn scenario_03_sink_depletion() {
    let case = case_json("03-sink-depletion");

    // Recipe pinned to `test_golden_rebalance_planner.py::SCENARIOS
    // ["profitable_dest_preferred"]` (golden generator: `test_golden_plan`,
    // sourcing `tests/golden/fixtures/rebalance/
    // plan_profitable_dest_preferred.json`, which `tools/conformance/
    // generate_scenarios.py::s03` copies verbatim as `expected`).
    let aaa = PlannerRecipeChannel {
        id: "aaa",
        local_ratio: 0.92,
        value_class: "active",
        capacity_sats: 2_000_000,
        remaining_budget_sats: 5_000,
    };
    let bbb = PlannerRecipeChannel {
        id: "bbb",
        local_ratio: 0.08,
        value_class: "profitable",
        capacity_sats: 2_000_000,
        remaining_budget_sats: 5_000,
    };
    let ccc = PlannerRecipeChannel {
        id: "ccc",
        local_ratio: 0.08,
        value_class: "neutral",
        capacity_sats: 2_000_000,
        remaining_budget_sats: 5_000,
    };

    let channels = [
        recipe_planner_channel(&aaa),
        recipe_planner_channel(&bbb),
        recipe_planner_channel(&ccc),
    ];
    let result = plan(&channels, 2_000_000, 10, 0);

    assert_eq!(result.pairs.len(), 1, "expected exactly one selected pair");
    let pair = &result.pairs[0];
    assert_eq!(pair.source, "aaa");
    assert_eq!(pair.dest, "bbb");

    let produced = json!({
        "drain_demand": drain_demand_wire(&result.drain_demand, result.pairs.len()),
        "selected": [selected_pair_wire(&aaa, &bbb, pair, 0.0, 0.0)],
        "skipped": result.skips.iter().map(skip_record_wire).collect::<Vec<_>>(),
    });
    assert_deep_eq_pyfloat(&produced, &case["expected"], "03-sink-depletion");
}

#[test]
fn scenario_14_hot_channel_priority() {
    let case = case_json("14-hot-channel-priority");
    let hot = mode("hot_protection").expect("hot_protection mode row");
    let normal = mode("normal").expect("normal mode row");
    let produced = json!({
        "hot_protection_priority": hot.priority,
        "normal_priority": normal.priority,
        "hot_beats_normal": hot.priority > normal.priority,
    });
    assert_canon_eq(&produced, &case["expected"], "14-hot-channel-priority");
}

#[test]
fn scenario_15_normal_rebalance() {
    let case = case_json("15-normal-rebalance");

    // Recipe pinned to `test_golden_rebalance_planner.py::SCENARIOS
    // ["amount_bounded_by_chunk"]` (golden generator: `test_golden_plan`,
    // sourcing `tests/golden/fixtures/rebalance/
    // plan_amount_bounded_by_chunk.json`, which `tools/conformance/
    // generate_scenarios.py::s15` copies verbatim as `expected`).
    let aaa = PlannerRecipeChannel {
        id: "aaa",
        local_ratio: 1.00,
        value_class: "active",
        capacity_sats: 50_000_000,
        remaining_budget_sats: 100_000,
    };
    let bbb = PlannerRecipeChannel {
        id: "bbb",
        local_ratio: 0.00,
        value_class: "active",
        capacity_sats: 50_000_000,
        remaining_budget_sats: 100_000,
    };

    let channels = [recipe_planner_channel(&aaa), recipe_planner_channel(&bbb)];
    let result = plan(&channels, 2_000_000, 10, 0);

    assert_eq!(result.pairs.len(), 1, "expected exactly one selected pair");
    let pair = &result.pairs[0];
    assert_eq!(pair.source, "aaa");
    assert_eq!(pair.dest, "bbb");
    assert!(
        result.skips.is_empty(),
        "expected no skips (both channels pair with each other)"
    );

    let produced = json!({
        "drain_demand": drain_demand_wire(&result.drain_demand, result.pairs.len()),
        "selected": [selected_pair_wire(&aaa, &bbb, pair, 0.0, 0.0)],
        "skipped": Vec::<Value>::new(),
    });
    assert_deep_eq_pyfloat(&produced, &case["expected"], "15-normal-rebalance");
}

#[test]
fn scenario_17_manual_diagnostic_rebalance() {
    let case = case_json("17-manual-diagnostic-rebalance");
    let manual = mode("manual").expect("manual mode row");
    let diagnostic = mode("diagnostic").expect("diagnostic mode row");
    let produced = json!({
        "manual": {
            "priority": manual.priority,
            "operator_directed": true,
        },
        "diagnostic": {
            "priority": diagnostic.priority,
            "bounded_spend": true,
        },
    });
    assert_canon_eq(
        &produced,
        &case["expected"],
        "17-manual-diagnostic-rebalance",
    );
}

/// Task 9 (T7-review ledgered obligation), pinned alongside scenario 17
/// per the phase plan's brief: the sats-EV gate's `failure_penalty_sats`
/// term must subtract in Python's EXACT left-to-right sequence
/// (`rebalance_engine_v2.py:556-562`), not folded into
/// `activity_penalty_sats` beforehand — floating-point subtraction is not
/// associative, and a 20_000-case randomized sweep against the real Python
/// function found the two orderings disagree in ~0.4% of cases after
/// `round(_, 6)` (see `revops_rebalance::ev`'s module doc comment). This
/// case (`fixtures/rebalance/ev.json`'s `failure_penalty_fold_cases`,
/// case_id `adversarial_sequence_vs_fold_disagreement`, generated by
/// `tools/port/gen_rebalance_fixtures.py::_gen_ev_failure_penalty_fold_cases`
/// in the port repo, branch `phase5-t9-gen`) is a hand-picked, REAL
/// divergence: the buggy fold order previously in `engine.rs` produced
/// `-323645.610578`; the fixed sequential order (and real Python) produce
/// `-323645.610579`. `tests/ev.rs` in `revops-rebalance` replays the full
/// 201-case set; this is the scenario-17-adjacent single-case pin the T7
/// review specifically called for.
#[test]
fn scenario_17_ev_failure_penalty_subtraction_order_pin() {
    let ev_inputs = EvInputs {
        probability_ppm: 0,
        dest_attempts: 2,
        dest_success_rate: 0.512_045_483_065_97,
        efv_sats: 2121.243,
        fee_sats: 215_427,
        source_opportunity_sats: 1377.7473285,
        failure_penalty_sats: 107_713.5,
        activity_penalty_sats: 187.98475,
        hold_margin_sats: 0.0,
    };
    let verdict = sats_ev_gate(&ev_inputs);

    // Real Python's `_build_score_decomposition` output for these exact
    // inputs (sequential subtraction) — NOT what the pre-fix folded order
    // produced (`-323645.610578`).
    assert_eq!(
        py_repr(verdict.final_score_sats),
        py_repr(-323_645.610579_f64),
        "sats_ev_gate must subtract failure_penalty_sats and \
         activity_penalty_sats as SEPARATE sequential terms, matching \
         Python's exact left-to-right order — a folded (activity + \
         failure) subtraction diverges here"
    );
}

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

// --- authorization (19, 22, 29, 30) ---

#[test]
fn scenario_19_protected_close_rejection() {
    let case = case_json("19-protected-close-rejection");
    // Recipe pinned by the task brief (`inputs` is `{}`):
    // `policy_close_block(Dynamic, ["protect"])`. Byte-compares against the
    // fixture's literal U+2014 EM DASH string.
    let reason = policy_close_block(&FeeStrategy::Dynamic, &["protect".to_string()]);
    let produced = json!({
        "allowed": reason.is_none(),
        "reason": reason,
    });
    assert_canon_eq(&produced, &case["expected"], "19-protected-close-rejection");
}

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

// --- reservation (23, 24) ---

#[test]
fn scenario_23_concurrent_reservation_contention() {
    let case = case_json("23-concurrent-reservation-contention");
    let inputs = &case["inputs"];
    let budget_sats = inputs["budget_sats"].as_i64().expect("budget_sats int");
    let reservations = inputs["reservations"]
        .as_array()
        .expect("inputs.reservations array");
    assert_eq!(
        reservations.len(),
        2,
        "23-concurrent-reservation-contention: expected exactly 2 reservations"
    );

    let dir = TempDir::new().unwrap();
    let mut db = BudgetDb::open(&dir.path().join("budget.db")).unwrap();

    let mut granted = Vec::with_capacity(reservations.len());
    for r in reservations {
        let (g, _remaining) = db
            .reserve_spend(
                ReserveRequest {
                    reservation_id: r["id"].as_str().expect("reservation id").to_string(),
                    amount_sats: r["amount_sats"].as_i64().expect("amount_sats int"),
                    category: "conformance".to_string(),
                    effective_budget_sats: Some(budget_sats),
                    ..ReserveRequest::default()
                },
                NOW,
            )
            .unwrap();
        granted.push(g);
    }

    let produced = json!({
        "first_granted": granted[0],
        "second_granted": granted[1],
    });
    assert_canon_eq(
        &produced,
        &case["expected"],
        "23-concurrent-reservation-contention",
    );
}

#[test]
fn scenario_24_restart_outstanding_reservation() {
    let case = case_json("24-restart-outstanding-reservation");
    let inputs = &case["inputs"];
    let reserved = &inputs["reserved_before_restart"];
    let rid = reserved["id"].as_str().expect("reservation id").to_string();
    let amount_sats = reserved["amount_sats"].as_i64().expect("amount_sats int");

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("budget.db");
    {
        let mut db = BudgetDb::open(&db_path).unwrap();
        let (granted, _) = db
            .reserve_spend(
                ReserveRequest {
                    reservation_id: rid.clone(),
                    amount_sats,
                    category: "conformance".to_string(),
                    ..ReserveRequest::default()
                },
                NOW,
            )
            .unwrap();
        assert!(
            granted,
            "24-restart-outstanding-reservation: initial reserve must be granted"
        );
        // `db` drops here, releasing the connection, before the SAME file is
        // reopened below — this is the "restart" the scenario name promises.
    }

    let db = BudgetDb::open(&db_path).unwrap();
    let states = db
        .get_spend_reservation_states(Some(std::slice::from_ref(&rid)))
        .unwrap();
    let state = states.get(&rid).unwrap_or_else(|| {
        panic!("24-restart-outstanding-reservation: {rid} missing after restart")
    });

    // TRAP (task brief): the fixture's expected value is a Python dict-REPR
    // STRING, not a JSON object: `str({'status': ..., 'reserved_sats': ...})`
    // — single quotes, `", "` between pairs, in THIS key order. Reproduced
    // exactly, not re-modeled as a JSON object.
    let state_repr = format!(
        "{{'status': '{}', 'reserved_sats': {}}}",
        state.status, state.reserved_sats
    );
    let mut state_after_restart = serde_json::Map::new();
    state_after_restart.insert(rid.clone(), Value::String(state_repr));
    let produced = json!({ "state_after_restart": Value::Object(state_after_restart) });
    assert_canon_eq(
        &produced,
        &case["expected"],
        "24-restart-outstanding-reservation",
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
