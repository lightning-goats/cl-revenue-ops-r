# Phase 3: Flow/Profitability/Policy + Budget Rail (revops-analytics + revops-db::budget) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Tasks are sized for one implementer subagent each and grouped into parallel waves (see Wave Table) for isolated worktrees per superpowers:using-git-worktrees.

**Goal:** Make the Rust plugin analytically complete and money-safe: a new `crates/revops-analytics` library crate porting the pure analysis layer (classification authority → Kalman flow filter → flow pipeline → profitability P&L → policy manager core → protection service → growth-budget math → datastore telemetry byte-shapes), plus THE BUDGET RAIL in `crates/revops-db` (new module `budget.rs`): the reserve/spend/release lifecycle over the production schema's `budget_reservations` + `spend_reservations` + `spend_events` tables with `BEGIN IMMEDIATE` atomicity and the P4-017 committed-total formula. Exit gate: no-double-spend concurrency tests (including across a simulated restart), the 9 Phase-3-owned conformance scenarios flipped from DEFERRED to REPLAYED and green, telemetry shapes byte-pinned, workspace green.

**Deadline context (2026-07-19 addendum):** Parallel workstream alongside Phase 4 (fees). Wave 0 ~1 hour; Wave 1 is 5-way parallel; total ~1 day. The reservation no-double-spend tests are on the design spec's NOT-compressed floor list — they must actually pass, never be waived.

**Python source of truth:** `~/bin/cl_revenue_ops-port` @ branch `port` (v2.18.1): `modules/classification.py`, `modules/flow_analysis.py`, `modules/demand_flow.py`, `modules/profitability_analyzer.py`, `modules/policy_manager.py`, `modules/protection_service.py`, `modules/growth_budget.py`, `modules/database.py` (lines 90–210 `_reserve_budget_atomic`, 973–1030 DDL, 3698–4340 reservation lifecycle), `modules/data_service.py::datastore_push`. Golden oracles: `tests/golden/fixtures/{profitability,close_protection}/`, driven by `tests/golden/test_golden_profitability.py` and `test_golden_close_protection.py` (`FROZEN_NOW = 1_752_400_000`).

## PRODUCTION-WRITE CONSTRAINT (read this twice)

**This phase writes the PRODUCTION SCHEMA SHAPE for the first time in Rust — but it NEVER writes the production database.** Operator ruling (design spec constraint 2): the Rust plugin never writes lnnode's `revenue_ops.db` and never holds action authority until an explicit per-subsystem flag cutover. Therefore, in this phase and until cutover:

- Every `revops-db::budget` write path operates ONLY on (a) the plugin's OWN parallel DB file (the `owner.rs` pattern — plugin-created, never the production path), or (b) throwaway COPIES of fixtures (`fixtures/fixture.db`, tempdir copies) in tests.
- No task in this plan opens the production DB read-write, adds a production-path default, or wires the rail to live RPC authority. Wiring + cutover rehearsal on a DB copy is a later, separately-gated step ("rehearse each cutover on a DB copy first" — spec risk register).
- Test hygiene: every write-path test constructs its DB via `tempfile::TempDir` + either the exact Python DDL (below) or a copy of `fixtures/fixture.db`. A test that takes a DB path from an env var must refuse paths outside the tempdir unless `#[ignore]`d and explicitly named `prod_copy`.

## Global Constraints

- Every file in `crates/revops-analytics`: crate root has `#![forbid(unsafe_code)]`; clippy warnings deny in CI. Same for the new `crates/revops-db/src/budget.rs`.
- **Workspace dep set only.** No new external crates. `revops-analytics` deps: `revops-core`, `revops-econ` (for `snapshot::Protection`, `types::UnixTime`, `pyfloat::{py_repr, py_round}`), `serde`, `serde_json`, `thiserror`. T1 adds `serde_json` and `revops-core` links to `crates/revops-db/Cargo.toml` as needed (workspace-declared already). No `rand` (fixture randomness is generated Python-side), no `nalgebra` (the Kalman filter is a hand-rolled 2x2 — transcribe, don't matrixify).
- **Floats are ALLOWED in this crate — quarantined by role.** Unlike revops-econ, the analytics layer is float-native (ratios, ROI, Kalman state, confidences). The rule is not "no floats", it is: **money stays integer msat/sats end-to-end** (`ChannelRevenue` is msat-native `i64`; the budget rail is sats-native `i64`); floats never flow INTO a budget/reservation amount; and every float computation is transcribed with Python's exact operation ORDER (no algebraic simplification, no `mul_add`, no re-association) so IEEE-754 doubles stay bit-identical. Fixtures pin f64 values as `u64` bit patterns (`f64::to_bits`), not decimal strings.
- **Rounding boundaries frozen:** msat→sat CEIL for fees/contributions with the "non-zero msat → at least 1 sat" guard (`fees_earned_sats`, `sourced_fee_contribution_sats`, `total_contribution_sats`); FLOOR (truncate) for volume; Python `//` is floor-toward-negative-infinity (use `i64::div_euclid` where the operand can be negative — `days_inactive`, `sourced_fee_raw // 1000`); Python `round(x, n)` is banker's — use `revops_econ::pyfloat::py_round` (24k-case cross-checked in Phase 2 T8), never `format!("{:.2}")`.
- **THREE JSON serializers coexist — never conflate them:**
  1. `revops_core::canonical::canonical_json` — sorted, compact, float-rejecting. Conformance comparisons and idempotency keys only.
  2. `revops_econ::ledger::python_dumps_default` semantics — `sort_keys=True`, separators `(", ", ": ")`, float-rejecting. Used in this phase for `spend_reservations.metadata_json` byte-compat (Python: `json.dumps(metadata or {}, sort_keys=True)`).
  3. **NEW (T8):** telemetry dumps — Python `json.dumps(payload)` defaults: **insertion order** (NOT sorted), separators `(", ", ": ")`, floats rendered via `py_repr`. This is the `["revenue", "profitability-summary"]` datastore byte shape.
- **Enum wire duality (trap):** the conformance corpus pins Python enum **NAMEs** (`"BALANCED"`, `"OUTBOUND_GATEWAY"`, `"STAGNANT_CANDIDATE"`, `"ZOMBIE"` — uppercase); the datastore telemetry pins enum **values** (`"balanced"`, `"stagnant_candidate"`, `"inbound_gateway"` — lowercase). Every enum in this crate gets BOTH `as_name()` and `as_value()`, each pinned by tests.
- Wire strings frozen: close-protection reasons (`"KALMAN_LOW_CONFIDENCE"`, `"INBOUND_GATEWAY"`, `"SOURCED_FEE_CONTRIBUTION"`, `"REVENUE_ROUTE"`), policy-block strings with the literal U+2014 em-dash (`"Channel tagged 'protect' — close blocked"`, `"Channel has static policy — close blocked"`), reservation statuses `'active'/'spent'/'released'`. Wording changes are conformance/golden failures.
- **sqlite discipline (matches revops-db house rules):** system sqlite (bundled OFF), `busy_timeout = 5000` on every connection, WAL assumed. `BEGIN IMMEDIATE` is issued as a raw statement (`conn.execute_batch("BEGIN IMMEDIATE")`) — NOT rusqlite's `Transaction` API, whose default is DEFERRED and whose Drop-rollback would fight the explicit COMMIT/ROLLBACK control flow ported from Python.
- Strict TDD: each task writes its failing tests first (transcribed Python behaviors + the golden/corpus values named in the task), then implements.
- **File discipline / parallel-safety:** Only T1 touches shared files (root `Cargo.toml`, `crates/revops-analytics/Cargo.toml` + `lib.rs`, `crates/revops-db/Cargo.toml` + the ONE `pub mod budget;` line in `crates/revops-db/src/lib.rs`, stub `budget.rs`). Only T10 touches `crates/revops-econ/tests/conformance.rs` and `crates/revops-econ/Cargo.toml` (dev-deps). **NOTHING in this plan touches `crates/revops-fees` (does not exist yet — Phase 4 owns it), `crates/revops-core`, `crates/revops-econ/src/**`, `crates/revops-rpc`, or `crates/revops`** — Phase 4 runs in parallel and the merge must be conflict-free.
- Config-key note (progress ledger, Phase1b T3+T4 rider): the 17 PUBLIC_RUNTIME_KEYS that are DB-override-only (`paused`, `risk_profile`, econ flags…) are owned by the config-membership task (1b T5/2b), NOT here. Everything in this phase takes config/clock/DB evidence as **injected values or closures** — no config lookups, no `time.time()` (mirrors the golden drivers' frozen-clock design and keeps every function replayable).

## Wave Table (parallelism)

| Wave | Tasks | Parallel-safe? |
|---|---|---|
| 0 | T1 scaffold (all shared-file edits) | sequential gate |
| 1 | T2 classification · T3 kalman · T4 profitability+growth · T5 policy+protection · T6 budget rail | 5-way parallel (disjoint files) |
| 2 | T7 flow pipeline (T2,T3) · T8 telemetry (T4) · T9 budget concurrency+restart (T6) · T10 conformance flip (T2,T4,T5,T6) | 4-way parallel (disjoint files) |
| 3 | T11 phase gate | gate |

Deferred out of this plan (explicit, not silent): FlowAnalyzer's RPC/DB *orchestration* (`analyze_all_channels` daemon loop, `_get_channels` via listpeerchannels, batched `update_channel_states_batch` writes) — Phase 3b wiring in `crates/revops` after the diff harness can compare `revenue-r-analyze` outputs; `BookkeeperCache`/bkpr open-cost forensics (`_get_open_cost_from_bookkeeper`, `_sanity_check_open_cost`) — needs live `bkpr-listaccountevents`, rides with the Phase 6 capital work (scenario 39 stays deferred: prose-only contract); `identify_bleeders_v2`/`get_tlv`/report RPCs — read-only reporting, Phase 3b/6; policy DB table (`upsert_policy`/`_row_to_policy` SQL) — the PeerPolicy *decision core* ports now, its persistence rides with the config workstream's DB actor; hourly forward histograms + `TemporalProfile` *consumers* (fee controller) — the pure `TemporalProfile` type ports in T7, its feeding query in Phase 3b.

---

### Task 1 (Wave 0): Scaffold — every shared-file edit in the phase

**Files:**
- Modify: `Cargo.toml` (root — add `crates/revops-analytics` to members)
- Modify: `crates/revops-db/Cargo.toml` (add `serde_json.workspace = true` to `[dependencies]`)
- Modify: `crates/revops-db/src/lib.rs` (ONE line: `pub mod budget;` — nothing else, ever, in this phase)
- Create: `crates/revops-db/src/budget.rs` (stub: doc comment stating the production-write constraint verbatim)
- Create: `crates/revops-analytics/Cargo.toml`
- Create: `crates/revops-analytics/src/lib.rs` declaring ALL modules for the whole phase — `classification`, `kalman`, `flow`, `demand_flow`, `profitability`, `growth`, `policy`, `protection`, `telemetry` — as stub files with doc comments only, so Waves 1–2 never edit shared files.

```toml
# crates/revops-analytics/Cargo.toml
[package]
name = "revops-analytics"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
revops-core = { path = "../revops-core" }
revops-econ = { path = "../revops-econ" }
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true

[dev-dependencies]
tempfile.workspace = true
```

- [ ] Failing test: a trivial `crates/revops-analytics/src/lib.rs` doc test / smoke test compiles; `cargo test -p revops-db` still green with the `budget` stub.
- [ ] Implement; `cargo clippy --workspace -- -D warnings` green. Commit. **After this commit, no other task edits any file another task owns.**

---

### Task 2 (Wave 1, parallel): Classification authority

**Mirrors:** `modules/classification.py` (whole file — it is already the pure, extracted decision authority; port 1:1).

**Files:** Create `crates/revops-analytics/src/classification.rs` (replace stub), `crates/revops-analytics/tests/classification.rs`; vendor `fixtures/golden/profitability/role30d_*.json` (byte-for-byte from `~/bin/cl_revenue_ops-port/tests/golden/fixtures/profitability/role30d_*.json` — 5 files).

**Interfaces:**
```rust
pub enum ChannelState { Source, Sink, Balanced, BalancedActive, Dormant, Unknown, Congested }
impl ChannelState {
    pub fn as_value(&self) -> &'static str; // "source" … "balanced_active" (Python .value)
    pub fn as_name(&self) -> &'static str;  // "SOURCE" … (Python .name — corpus wire)
    pub fn is_balanced(&self) -> bool;      // Balanced | BalancedActive
}
pub enum ChannelRole { InboundGateway, OutboundGateway, Balanced, Dormant } // + as_value/as_name

// Constants verbatim (classification.py lines 82–92):
pub const BALANCED_ACTIVE_TURNOVER_THRESHOLD: f64 = 0.01;
pub const DORMANT_KALMAN_RATIO_THRESHOLD: f64 = 0.01;
pub const SINK_ENTER_OUTBOUND_RATIO: f64 = 0.78;
pub const SINK_EXIT_OUTBOUND_RATIO: f64 = 0.72;
pub const SOURCE_ENTER_OUTBOUND_RATIO: f64 = 0.22;
pub const SOURCE_EXIT_OUTBOUND_RATIO: f64 = 0.28;
pub const KALMAN_BALANCE_VETO_RATIO: f64 = 0.05;
pub const ROLE_MIN_FORWARDS_30D: i64 = 10;
pub const ROLE_DIRECTIONAL_RATIO: f64 = 0.70;

/// F1 hysteresis: band choice keys on the PREVIOUS class (case-insensitive,
/// None → ""), F1c Kalman direction veto, then turnover → BALANCED_ACTIVE,
/// |kalman| < 0.01 → DORMANT, else BALANCED. Strict > / < comparisons as in Python.
pub fn classify_balance_position(outbound_ratio: f64, previous_state: Option<&str>,
                                 kalman_ratio: f64, turnover: f64) -> ChannelState;
pub fn flow_state(kalman_ratio: f64, source_threshold: f64, sink_threshold: f64,
                  outbound_ratio: f64, previous_state: Option<&str>, turnover: f64) -> ChannelState;
/// role_30d decision: !window_30d_available → lifetime_role; total < 10 → DORMANT;
/// inbound_ratio > 0.70 → INBOUND_GATEWAY; outbound_ratio > 0.70 → OUTBOUND_GATEWAY; else BALANCED.
pub fn revenue_role_30d(window_30d_available: bool, forward_count_30d: i64,
                        sourced_forward_count_30d: i64, lifetime_role: ChannelRole) -> ChannelRole;
```

- [ ] Failing tests first: the 5 vendored `role30d_*` goldens replayed exactly (inputs from each fixture's `inputs` key; note fixture stores enum NAMES); hysteresis table transcribed from Python's flow-analysis suite — previous `sink` holds until outbound_ratio ≤ 0.72, fresh channel enters SINK only above 0.78, same asymmetry for SOURCE; kalman veto both directions (outbound 0.9 + kalman +0.06 → NOT SINK); turnover boundary `0.01` exact (strict >, so exactly 0.01 is NOT balanced_active); dormant needs `abs(kalman) < 0.01`; `flow_state` threshold strictness (`kalman > source_threshold` — equality falls through to balance position). Drift-guard test: vendored fixture bytes == port-worktree source bytes when available (same pattern as conformance's `corpus_is_byte_identical_to_source`).
- [ ] Implement; commit.

---

### Task 3 (Wave 1, parallel): Kalman flow filter — bit-parity float math

**Mirrors:** `modules/flow_analysis.py` — `KalmanFlowState` (`to_dict`/`from_dict` with `_safe` non-finite fallback), `KalmanFlowFilter` (predict/update/`_ensure_positive_definite`/`get_uncertainty`/`is_regime_change`/NaN recovery), `_calculate_kalman_volatility`, `_compute_raw_kalman_observation`, `_calculate_confidence`, `estimate_depletion_hours`, and ALL `KALMAN_*` constants (lines 69–129).

**Files:** Create `crates/revops-analytics/src/kalman.rs`, `crates/revops-analytics/tests/kalman.rs`, `fixtures/kalman.json`; in the PORT worktree create `tools/port/gen_kalman_fixtures.py` (commit there, copy JSON here — established convention).

**The tricky contract in full — transcribe, do not "fix":**
```rust
pub const KALMAN_MAX_VELOCITY: f64 = 0.5 / 24.0;      // keep the DIVISION — do not fold constants
pub const KALMAN_MIN_UPDATE_INTERVAL_SECS: i64 = 300;  // the no-touch gate (T7 consumes it)
// … all constants as the same arithmetic expressions Python uses.

#[derive(Clone, Debug, PartialEq)]
pub struct KalmanFlowState {
    pub flow_ratio: f64, pub flow_velocity: f64,
    pub variance_ratio: f64, pub variance_velocity: f64, pub covariance: f64,
    pub last_update: i64, pub innovation_variance: f64, pub last_innovation: f64,
    pub observation_count: i64,
}
pub struct KalmanFlowFilter { pub state: KalmanFlowState, nan_recovery_count: u32 }
impl KalmanFlowFilter {
    /// dt<=0 → return; NaN anywhere → reset (fresh default state) and return.
    /// flow_ratio += velocity*dt; clamp ratio [-1,1], velocity [min,max];
    /// q_ratio = BASE*volatility*2.0 clamped; q_velocity = VEL_BASE*volatility clamped to [MIN/10, MAX/10];
    /// covariance prediction EXACTLY:
    ///   new_p00 = p00 + 2*dt*p01 + dt*dt*p11 + q_ratio*dt + q_velocity*dt*dt*dt/3.0
    ///   new_p01 = p01 + dt*p11 + q_velocity*dt*dt/2.0
    ///   new_p11 = p11 + q_velocity*dt
    /// then _ensure_positive_definite (floor 1e-4 each variance; if det<=0
    /// clamp covariance to ±0.9*sqrt(p00*p11)).
    pub fn predict(&mut self, dt_hours: f64, volatility: f64);
    /// Non-finite observation → return 0.0 WITHOUT touching state (B2 fix).
    /// r = 0.05 / max(0.1, confidence*0.8), clamp [0.01, 0.5];
    /// innovation = z - flow_ratio; s = p00 + r (floor 1e-10);
    /// k0 = p00/s; k1 = p01/s; state += K*innovation;
    /// Joseph-ish form — NOTE new_p01 is NOT the textbook symmetric formula,
    /// transcribe verbatim (flow_analysis.py lines 601–603):
    ///   new_p00 = (1-k0)*p00*(1-k0) + k0*k0*r
    ///   new_p01 = (1-k0)*p01 - k1*(1-k0)*p00 + k0*k1*r
    ///   new_p11 = p11 - k1*p01 - k1*(p01 - k1*p00) + k1*k1*r
    /// then positive-definite fix, THEN clamp ratio/velocity (order matters);
    /// last_innovation = innovation;
    /// innovation_variance = max(0.001, 0.9*iv + 0.1*innovation*innovation);
    /// last_update = now (INJECTED — see below); observation_count += 1;
    /// NaN check LAST → reset + return 0.0.
    pub fn update(&mut self, observed_ratio: f64, confidence: f64, now: i64) -> f64;
    pub fn get_uncertainty(&self) -> f64;              // sqrt(max(0, p00))
    pub fn is_regime_change(&self, threshold: f64) -> bool; // |li| > t*sqrt(max(0.001, iv))
    pub fn take_nan_recovery_count(&mut self) -> u32;
}
/// Clock injection: Python calls int(time.time()) INSIDE update(); Rust takes
/// `now` as a parameter (golden drivers freeze the clock the same way).
/// Volatility: <3 buckets → 1.0; mean_flow < 1000 → 0.5; else 0.5 + min(1.5, cv*3.0)
/// with cv = mean_change / max(1, mean_flow) — note max(1,…) is on a FLOAT in
/// Python (f64::max(1.0, …)).
pub fn calculate_kalman_volatility(daily_buckets: &[DailyBucket]) -> f64;
/// 24h rolling window over (timestamp, net_msat) entries; net_sats = sum(net_msat/1000.0)
/// — FLOAT division per entry, then sum, NOT integer sum then divide. ratio clamped [-1,1].
pub fn compute_raw_kalman_observation(capacity: i64, entries: &[NetFlowEntry], now: f64) -> (f64, usize);
pub fn calculate_confidence(forward_count: i64, last_forward_ts: i64, now: i64) -> f64;
pub fn estimate_depletion_hours(...) -> Option<f64>;   // unit contract from lines 166–210
```

**Fixture generator (`tools/port/gen_kalman_fixtures.py`):** seeded Python driver producing ~300 cases: fresh + persisted-state filters through randomized `predict`/`update` interleavings (dt 0.01–200h, volatility 0.4–2.2, observations in [-1.5, 1.5] incl. NaN/inf rejects, confidences 0–1.2), dumping after each step the full state as **`struct.pack('<d', v).hex()`** bit patterns + the returned innovation. Rust test replays every sequence and asserts `f64::to_bits` equality — no epsilon. Also fixture-pin `to_dict`/`from_dict`: `_safe` replaces missing/non-finite fields per-key with that key's default; roundtrip of a NaN-poisoned dict yields defaults.

- [ ] Failing tests first (fixture replay + hand cases: dt=0 no-op; NaN state → reset counted once; det<=0 covariance clamp; observation NaN leaves state untouched but Python still returns 0.0).
- [ ] Implement; commit (port-worktree commit for the generator, this-repo commit for fixture + code).

---

### Task 4 (Wave 1, parallel): Profitability P&L core + growth budget math

**Mirrors:** `modules/profitability_analyzer.py` — `ProfitabilityClass`, `ChannelCosts`, `ChannelRevenue` (msat-native + sat properties), `ChannelProfitability` (incl. `marginal_roi`, `marginal_roi_percent`, `is_operationally_profitable`, `channel_role`, `role_30d` via T2's authority — but T4 re-implements only the *evidence prep*, delegating the decision to `classification::revenue_role_30d`; to avoid a Wave-1 cross-dependency, code against the T1 stub signature which T2 fills), `marginal_roi_reliable`, `_classify_channel` (pure-ified), `_days_since_routed`. Plus `modules/growth_budget.py` (whole file — already pure).

**Files:** Create `crates/revops-analytics/src/profitability.rs`, `src/growth.rs`, `crates/revops-analytics/tests/profitability.rs`, `tests/growth.rs`; vendor `fixtures/golden/profitability/classify_*.json` + `marginal_roi_*.json` (byte-for-byte; disjoint from T2's `role30d_*` files).

**Interfaces + tricky contracts:**
```rust
pub enum ProfitabilityClass { Profitable, BreakEven, Underwater, StagnantCandidate, Zombie }
// as_value: "profitable"… ; as_name: "PROFITABLE", "STAGNANT_CANDIDATE", "ZOMBIE" (corpus wire)

pub struct ChannelRevenue { // msat-native i64, EXACT field set (lines 200–207)
    pub channel_id: String,
    pub fees_earned_msat: i64, pub volume_routed_msat: i64, pub forward_count: i64,
    pub sourced_volume_msat: i64, pub sourced_fee_contribution_msat: i64, pub sourced_forward_count: i64,
}
impl ChannelRevenue {
    pub fn fees_earned_sats(&self) -> i64;   // <=0 → 0; else CEIL msat→sat (nonzero → >=1)
    pub fn volume_routed_sats(&self) -> i64; // FLOOR
    pub fn sourced_fee_contribution_sats(&self) -> i64; // <=0 → 0; else CEIL
    /// max(earned, sourced) — VALUATION only. Fleet revenue is sum of
    /// fees_earned across EXIT channels (bkpr attribution lesson: the exit
    /// channel earns the fee; sourced_* is entry-side attribution for
    /// protection/valuation, never summed into fleet revenue — double count).
    pub fn total_contribution_msat(&self) -> i64;
    pub fn total_forward_count(&self) -> i64;
}

pub struct ChannelProfitability { /* all fields from lines 288–317, exact names incl.
    marginal_profit_30d_sats, rebalance_cost_30d_sats, opener,
    contribution_30d_msat, fees_earned_30d_msat, sourced_fee_30d_msat,
    forward_count_30d, sourced_forward_count_30d, window_30d_available */ }
impl ChannelProfitability {
    /// cost<=0 → 1.0 if profit>0 else 0.0; else profit / max(1, cost) as f64
    /// TRUE division (i64 as f64 / i64 as f64) — corpus s05 pins -300/600 = -0.5.
    pub fn marginal_roi(&self) -> f64;
    pub fn marginal_roi_percent(&self) -> f64;      // * 100.0
    pub fn marginal_roi_reliable(&self) -> bool;    // rebalance_cost_30d_sats >= 100
    pub fn channel_role(&self) -> ChannelRole;      // lifetime: <10 total → DORMANT; >0.70 gates
    pub fn role_30d(&self) -> ChannelRole;          // delegates to classification::revenue_role_30d
    pub fn total_forward_count_30d(&self) -> i64;
}

/// _classify_channel, pure-ified: clock + diagnostic evidence + fee-state
/// variance INJECTED (Python reads DB + wall clock inline).
pub struct DiagStats { pub attempt_count: i64, pub last_success_time: i64 } // Python None → 0
pub struct ClassifyEvidence<'a> {
    pub now: i64,
    pub diag_stats: Option<&'a DiagStats>,          // only consulted when roi < underwater
    pub posterior_variance: Option<f64>,            // from v2_state_json thompson_state; None → 10000-equivalent (no widening)
    pub contribution_30d_msat: Option<i64>,         // None disables the F3 corpse branch
}
pub fn classify_channel(roi: f64, _net_profit: i64, last_routed: Option<i64>,
                        days_open: i64, forward_count: i64,
                        ev: &ClassifyEvidence) -> ProfitabilityClass;
```

Branch order to port verbatim (lines 2659–2755): days_inactive from `(now - last_routed) // 86400` else `days_open`; ZOMBIE only when `roi < -0.10` AND attempts ≥ 2 AND (success>0 and hours_since>48 and (`!last_routed || last_routed < last_success`)) OR (success==0 and days_inactive ≥ 7); STAGNANT when `days_inactive >= 7 && roi < -0.10`; DTS widening when `posterior_variance < 2500.0`: `profitable_thresh *= 0.5`, `underwater_thresh *= 1.5` (thresholds 0.10 / -0.10 as consts); F3 corpse branch (`contribution<=0 && days_inactive>=30 && days_open>60` → STAGNANT); final strict `roi > profitable_thresh` / `roi < underwater_thresh`. Note the Python truthiness traps: `if last_routed:` treats **0 as never-routed**; `last_success_time or 0`.

`growth.rs`: `compute_growth_budget_status` ported 1:1 (both dict shapes — key ORDER preserved for telemetry; return a `Vec<(String, Value)>`-backed ordered struct or an ordered builder shared with T8), `_fleet_prior_status` reason strings frozen (`"missing"/"unusable"/"insufficient_samples"/"malformed_ratio"/"non_positive_prior"/"positive_prior"`), `beneficial_ratio` rounded `py_round(r, 4)`, credits via `floor`, `effective = min(max(base, uncapped), hard_ceiling)`, bool-rejection in `_safe_int`/`_fraction`.

- [ ] Failing tests first: the 4 `classify_*` goldens + zombie golden (evidence: attempts=2, success=0, roi -0.40, last_routed = 1_752_400_000 − 86_400*30, days 200, fwd 5 → ZOMBIE); the 4 `marginal_roi_*` goldens (compare via `f64::to_bits` where fixture stores a float); sat-property boundary table `[0, 1, 999, 1000, 1001]` msat → ceil `[0,1,1,1,2]` / floor `[0,0,0,1,1]`; growth-budget: enabled/disabled shapes, prior gates (sample_count < 3, ratio ≤ 0.5, bool ratio malformed), hard-ceiling cap flag.
- [ ] Implement; commit.

---

### Task 5 (Wave 1, parallel): Policy core + protection service

**Mirrors:** `modules/policy_manager.py` — `FeeStrategy`, `RebalanceMode`, `PeerPolicy` (has_tag/is_expired/get_fee_multiplier_bounds/to_dict), the `set_policy` VALIDATION core (pure-ified: existing policy + inputs → validated new policy or frozen error strings), peer-id pattern (`\A[0-9a-fA-F]{66}\Z` — anchored, rejects trailing newline). `modules/protection_service.py` (whole file): thresholds, `ChannelLifecycle`, `inactivity_is_signal`, `close_protection_reason`, `policy_close_block`, `lnplus_contract_protection`, `close_protections`, `derive_lifecycle`.

**Files:** Create `crates/revops-analytics/src/policy.rs`, `src/protection.rs`, `crates/revops-analytics/tests/protection.rs`, `tests/policy.rs`; vendor `fixtures/golden/close_protection/*.json` (byte-for-byte, all files).

**Interfaces:**
```rust
// policy.rs
pub enum FeeStrategy { Dynamic, Static, Passive }       // values "dynamic"/"static"/"passive"
pub enum RebalanceMode { Enabled, Disabled, SourceOnly, SinkOnly }
pub struct PeerPolicy { /* exact fields lines 95–104; tags: Vec<String> */ }
impl PeerPolicy {
    pub fn has_tag(&self, tag: &str) -> bool;
    pub fn is_expired(&self, now: i64) -> bool;          // expires_at None → false; strict now > expires_at
    pub fn fee_multiplier_bounds(&self) -> (f64, f64);   // global clamp + swap-if-inverted
}
/// set_policy validation core: every ValueError string frozen
/// ("Invalid strategy '{s}'. Valid: {list}", "strategy=static requires fee_ppm_target",
///  "fee_ppm_target cannot exceed 100000 PPM", the L-R5-8 min>max message, …).
/// Strategy/mode parse is LOWERCASED first. Rate limiting + DB write + cache stay
/// with the future wiring layer; this validates and produces the new PeerPolicy.
pub fn validate_policy_update(existing: &PeerPolicy, update: &PolicyUpdate, now: i64)
    -> Result<PeerPolicy, PolicyError>;
pub fn is_valid_peer_id(s: &str) -> bool;

// protection.rs — evidence structs make Python's duck-typing explicit:
pub const KALMAN_CONFIDENCE_FLOOR: f64 = 0.5;
pub const INBOUND_GATEWAY_ROI_FLOOR_PCT: f64 = -30.0;
pub const SOURCED_FEE_PROTECT_SATS: i64 = 100;
pub const SOURCED_FEE_ROI_FLOOR_PCT: f64 = -50.0;
pub const REVENUE_ROUTE_ROI_FLOOR_PCT: f64 = -30.0;

pub enum ChannelLifecycle { Candidate, Opening, Evaluating, Productive, Protected,
                            Underperforming, Recycling, Closing } // values lowercase
pub struct ProtProfEvidence { pub role_30d: Option<ChannelRole>, pub lifetime_role: ChannelRole,
    pub marginal_roi_percent: f64, pub window_30d_available: bool,
    pub sourced_fee_30d_msat: i64, pub lifetime_sourced_fee_sats: i64, pub days_open: i64 }
pub struct FlowEvidence { pub confidence: Option<f64>, pub forward_count: Option<i64> }
pub fn inactivity_is_signal(forward_count: Option<i64>, days_open: Option<i64>,
                            flow_window_days: i64) -> bool; // unparseable → false (keep gate)
pub fn close_protection_reason(scid_display: &str, prof: &ProtProfEvidence,
    flow: Option<&FlowEvidence>, route_pair_channels: &std::collections::BTreeSet<String>,
    flow_window_days: i64) -> Option<&'static str>;
pub fn policy_close_block(strategy: &FeeStrategy, tags: &[String]) -> Option<String>;
pub fn lnplus_contract_protection(opened_at: Option<i64>, duration_months: Option<i64>)
    -> Option<Protection>;                                  // revops_econ::snapshot::Protection
pub fn close_protections(...) -> Vec<Protection>;           // owners "close_protection"/"operator_policy"/"lnplus"
pub fn derive_lifecycle(staged_for_close: bool, opening: bool,
    protections: &[Protection], underperforming: bool) -> ChannelLifecycle;
```

**Gate-order + truthiness traps to port verbatim:** (1) Kalman gate FIRST — `confidence or 1.0` means **confidence 0.0 is treated as 1.0 (no gate)**, non-numeric → 1.0; gate fires only when `< 0.5` AND NOT `inactivity_is_signal` (zero forwards + `days_open > window+7`). (2) Gateway gate: `role_30d` first, `channel_role` fallback; protected when `marginal_roi_percent >= -30.0` (inclusive). (3) Sourced-fee: 30d window when `window_30d_available is True` (strict bool), `sourced_fee_30d_msat // 1000` (floor division); else lifetime fallback; protected when `> 100` sats AND roi `> -50.0` (both strict). (4) Route-pair: membership + roi `> -30.0` strict. First matching reason WINS — order is the contract. `policy_close_block` order: static → passive → protect → no_close ("protect" checked before "no_close"; tag priority pinned by the golden). Protection tag strings contain a literal em-dash.

- [ ] Failing tests first: replay ALL vendored close_protection goldens through the two functions (`allowed_*` fixtures via `policy_close_block` → `(reason.is_none(), reason)`; the rest via `close_protection_reason` with evidence transcribed from `test_golden_close_protection.py::SCENARIOS` — these evidence tables are copied into the Rust test as constants and cross-checked against the fixture bytes); the 5e8f747 anchor (stale lifetime sourcing NOT used when window present); fail-safe fallback (no window + lifetime 500 → protected); `confidence == 0.0` → no gate (truthiness pin); lifecycle precedence (recycling > opening > protected > underperforming > productive); lnplus expiry `start + months*30*86400`; peer-id trailing-newline reject.
- [ ] Implement; commit.

---

### Task 6 (Wave 1, parallel): THE BUDGET RAIL — `revops-db::budget`

**Mirrors:** `modules/database.py` — DDL lines 973–1030 (`budget_reservations`, `spend_reservations`, `spend_events` + indexes), `_reserve_budget_atomic` (lines 90–210, retained for parity), `reserve_spend` (3941–4106, THE implementation), `release_spend_reservation`, `mark_spend_reservation_spent` (P2-003 atomic), `record_spend_event` (P2-008 retry), `get_spend_reservation_states`, `get_category_spend_sats`, `cleanup_stale_reservations` (P4-015) + `cleanup_stale_spend_reservations` (P4-021), `count_stale_reservations`, `clear_all_reservations` (I-1), `release_budget_reservation`/`mark_budget_spent`/`reserve_budget` (Phase 2J dual-path wrappers), `_COMMITTED_ONCHAIN_SPEND_CATEGORIES = ("channel_open","channel_close","boltz")`.

**Files:** Create (replace stub) `crates/revops-db/src/budget.rs`, `crates/revops-db/tests/budget.rs`. NOTHING else — `lib.rs` already declares the module (T1).

**REPEAT THE CONSTRAINT:** this is the first Rust code that writes the production schema SHAPE. During shadow it operates on the plugin's own files and fixture copies ONLY. Every test in this task builds its DB in a tempdir (fresh DDL below, or a copy of `fixtures/fixture.db`). No production writes until cutover.

**The committed-total contract in full (this is the money):**
```rust
pub struct BudgetDb { conn: rusqlite::Connection }  // single-owner; the async actor wrap is 3b wiring
impl BudgetDb {
    /// Opens/creates with busy_timeout 5000 and the EXACT Python DDL:
    ///   budget_reservations(reservation_id TEXT PRIMARY KEY, reserved_sats INTEGER NOT NULL,
    ///     reserved_at INTEGER NOT NULL, job_channel_id TEXT NOT NULL,
    ///     status TEXT NOT NULL DEFAULT 'active')
    ///   spend_reservations(reservation_id TEXT PRIMARY KEY, category TEXT NOT NULL,
    ///     subcategory TEXT, reserved_sats INTEGER NOT NULL, reserved_at INTEGER NOT NULL,
    ///     reference_id TEXT, channel_id TEXT, status TEXT NOT NULL DEFAULT 'active',
    ///     metadata_json TEXT)
    ///   spend_events(event_id TEXT PRIMARY KEY, category TEXT NOT NULL, subcategory TEXT,
    ///     amount_sats INTEGER NOT NULL, timestamp INTEGER NOT NULL, …)  [+ 3 indexes, verbatim]
    /// Also creates minimal rebalance_costs(cost_sats INTEGER, timestamp INTEGER) when absent
    /// (the committed sums read it; production already has it — CREATE IF NOT EXISTS only).
    pub fn open(path: &Path) -> Result<Self, BudgetError>;

    /// reserve_spend — ONE BEGIN IMMEDIATE wraps guard + sums + insert (P2-003).
    /// Semantics, verbatim from Python:
    ///  * sanitize: amount<=0 / empty rid / empty category → (false, 0) — NO transaction begun
    ///    for the sanitize rejects? NO — Python sanitizes BEFORE "BEGIN IMMEDIATE". Mirror that.
    ///  * category lowercased+trimmed.
    ///  * terminal guard: existing status 'spent'/'released' → **COMMIT (not rollback)** + refuse
    ///    (never resurrect a terminal rid — double count). status 'active' → read its
    ///    reserved_sats as existing_active_sats (INSERT OR REPLACE re-reserve counts only the delta).
    ///  * committed-total (enforce only when effective_budget_sats is Some) — THE P4-017 SHAPE:
    ///      gen_reserved  = SUM(reserved_sats) spend_reservations  WHERE status='active'   -- NO time filter
    ///      reb_reserved  = SUM(reserved_sats) budget_reservations WHERE status='active'   -- NO time filter
    ///      reb_committed = SUM(cost_sats)   rebalance_costs WHERE timestamp >= since      -- windowed
    ///      gen_committed = SUM(amount_sats) spend_events    WHERE timestamp >= since      -- windowed
    ///      already = gen_reserved + reb_reserved + reb_committed + gen_committed - existing_active_sats
    ///      daily_remaining = effective_budget_sats - already
    ///      amount > daily_remaining → ROLLBACK, (false, daily_remaining)
    ///    Weekly (both weekly args Some): weekly_spent/weekly_gen_spent re-windowed on the weekly
    ///    since; the RESERVED sums are the SAME unfiltered values (held budget counts in full
    ///    regardless of age — P4-017). Reject → ROLLBACK, (false, weekly_remaining).
    ///  * INSERT OR REPLACE (…status 'active', reserved_at = now [injected clock]) → COMMIT.
    ///  * return remaining = daily_remaining - amount, min'd with weekly_remaining - amount.
    ///  * cross-category serialization: BEGIN IMMEDIATE takes the single WAL writer lock up
    ///    front, so this check is serialized against every other reserve — the DD1/P1-003
    ///    invariant "the two categories can never jointly overshoot" is the T9 test target.
    pub fn reserve_spend(&mut self, req: ReserveRequest, now: i64) -> Result<(bool, i64), BudgetError>;

    pub fn release_spend_reservation(&mut self, rid: &str) -> Result<bool, BudgetError>; // UPDATE …='released' WHERE …='active'
    /// P2-003: SELECT row → UPDATE 'spent' WHERE 'active' → (record_event: INSERT spend_event
    /// event_id "resv:{rid}", source default "reservation_settlement") in ONE BEGIN IMMEDIATE;
    /// event write failure → ROLLBACK and return the error — a reservation is NEVER left
    /// 'spent' without its event (fail toward HOLDING budget, the safe direction).
    pub fn mark_spend_reservation_spent(&mut self, rid: &str, actual_spent_sats: Option<i64>,
                                        source: Option<&str>, record_event: bool, now: i64)
                                        -> Result<bool, BudgetError>;
    /// P2-008: amount<=0 reject; INSERT OR REPLACE retried 3x on SQLITE_BUSY/LOCKED with
    /// 50ms/100ms/150ms sleeps, then Err (never a silent lost write — a lost spend event
    /// under-counts the budget in the OVERSPEND direction).
    pub fn record_spend_event(&mut self, ev: SpendEvent) -> Result<(), BudgetError>;
    pub fn get_spend_reservation_states(&self, ids: Option<&[String]>)
        -> Result<BTreeMap<String, ReservationState>, BudgetError>; // ORDER BY reservation_id, cap 10000
    pub fn get_category_spend_sats(&self, category: &str, subcategory: Option<&str>, since: i64) -> Result<i64, BudgetError>;
    /// P4-015: legacy sweep releases active budget_reservations older than cutoff EXCEPT those
    /// whose reservation_id matches CAST(rebalance_history.id AS TEXT) with status
    /// 'pending_settlement' (in-flight HTLC holds its budget); unified sweep same skip for
    /// category='rebalance'. rebalance_history absence tolerated (OperationalError pass — Python).
    pub fn cleanup_stale_reservations(&mut self, max_age_seconds: i64, now: i64) -> Result<i64, BudgetError>;
    /// P4-021: blind sweep skips ("channel_open","channel_close","boltz"); explicit category
    /// sweep reaches everything.
    pub fn cleanup_stale_spend_reservations(&mut self, max_age_seconds: i64, category: Option<&str>, now: i64) -> Result<i64, BudgetError>;
    pub fn clear_all_reservations(&mut self) -> Result<ClearStats, BudgetError>; // I-1: BEGIN IMMEDIATE read+update
    pub fn count_stale_reservations(&self, max_age_seconds: i64, now: i64) -> Result<i64, BudgetError>;
    // Phase 2J compatibility wrappers (rebalance callers):
    pub fn reserve_budget(&mut self, …) -> Result<(bool, i64), BudgetError>; // → reserve_spend(category="rebalance", _return_remaining)
    pub fn release_budget_reservation(&mut self, rid: &str) -> Result<bool, BudgetError>; // unified first, legacy-table fallback
    pub fn mark_budget_spent(&mut self, rid: &str, actual_spent: i64) -> Result<bool, BudgetError>; // record_event=false (costs live in rebalance_costs)
}
/// metadata_json byte-compat: json.dumps(metadata, sort_keys=True) with DEFAULT
/// separators (", ", ": ") — same semantics as revops_econ's python_dumps_default
/// (re-implement locally, ~20 lines; floats rejected — metadata is caller data).
```

- [ ] Failing tests first (tempdir DBs): committed-total arithmetic table — seed rows across all four sources, assert exact `remaining` on grant and on refusal (incl. the aged-active-reservation case P4-017 fixed: reservation `reserved_at` 10 days old still counts in full); re-reserve active rid replaces amount and only the delta gates; terminal rid refuses AND leaves status untouched (and the tx COMMITs — assert no lingering write lock by immediately writing from a second connection); weekly-cap rejection uses windowed spends + unfiltered holds; zero/negative amount and empty rid/category rejects; spent-without-event impossible (inject an event failure via a UNIQUE-violation shim or read-only page — simplest: pre-insert a conflicting spend_events row type mismatch is not enough, use a second connection holding the write lock so the INSERT busy-times-out with retries exhausted → assert reservation still 'active'); P4-015 pending-settlement skip (create rebalance_history minimal table in the test); P4-021 blind-vs-explicit sweep; wrapper dual-path fallback (legacy `budget_reservations` row created by hand releases via `release_budget_reservation`).
- [ ] Implement; `cargo test -p revops-db` green; commit.

---

### Task 7 (Wave 2, parallel; needs T2+T3): Flow pipeline + demand-flow classifier

**Mirrors:** `modules/flow_analysis.py` — `FlowMetrics` (+ `to_dict` with `py_round(…, 4)` fields), `_apply_kalman_reclassification` pure-ified (convergence gate `uncertainty < 0.25 && obs_count >= 5`, DTS widening `posterior_variance > 10000 → thresholds *= 1.5`, `outbound_ratio = our_balance/capacity` else 0.5, `turnover = daily_volume/capacity`), the 300s no-touch gate semantics as a pure decision (`KalmanStep::Untouched | PredictOnly | Updated` so the 3b wiring can decide what to persist), `_calculate_velocity`, `_calculate_adaptive_decay`, `_calculate_ema_flow`, `TemporalProfile` (+ `update_temporal_profile`, `graduated`). `modules/demand_flow.py` (whole file).

**Files:** Create `crates/revops-analytics/src/flow.rs`, `src/demand_flow.rs`, `crates/revops-analytics/tests/flow.rs`, `tests/demand_flow.rs`; port-worktree `tools/port/gen_flow_fixtures.py` → `fixtures/flow.json` (EMA/velocity/decay/temporal sequences + full `_apply_kalman_reclassification` cases, f64s as bit patterns).

Key contracts: `FlowMetrics.to_dict` rounds `kalman_*` to 4 via banker's (`py_round`) — pin against Python bytes; predict-only path still bumps `last_update` (dt accounting) and IS persisted, the `Untouched` path is NOT (mirrors `state_snapshot = None`); first-run `dt = 24.0`, cap `168.0`; demand-flow `classify_peers` ratio `(in-out)/total` with ±0.3 role bands, `confidence = clamp(0.3*log10(max(total,1))/log10(1e6))` to `[0.1, 0.9]`, `py_round(conf,3)`/`py_round(ratio,4)`; `classify_candidate` keyword tables + structure/fee heuristics verbatim (msat thresholds `500_000_000`/`5_000_000_000`, `total_cap_msat // count` integer division); `find_sink_adjacent_candidates` scoring `0.4 * conf * (1 + (n-rank)/n)` with `py_round(score,4)`, top-5 sinks/top-10 candidates, insertion-order dedup.

- [ ] Failing tests first (fixture replay + keyword/threshold tables + sort stability: Python `list.sort` is stable — use `sort_by` with the same key, pin a tie case).
- [ ] Implement; commit.

---

### Task 8 (Wave 2, parallel; needs T4): Frozen datastore telemetry byte-shapes

**Mirrors:** `modules/profitability_analyzer.py::_push_profitability_summary` (the `["revenue", "profitability-summary"]` payload — lines 700–742) + `modules/data_service.py::datastore_push` envelope rules (timestamp injection only when absent, error-key reject, size cap `_DATASTORE_MAX_BYTES`, `json.dumps(payload)` DEFAULT settings).

**Files:** Create `crates/revops-analytics/src/telemetry.rs`, `crates/revops-analytics/tests/telemetry.rs`, `fixtures/telemetry.json`; port-worktree `tools/port/gen_telemetry_fixtures.py`.

**The byte-shape contract:**
```rust
/// Python json.dumps DEFAULTS: keys in INSERTION order (dict literal order — NOT
/// sorted), separators (", ", ": "), floats via py_repr, ensure_ascii=True
/// (non-ASCII escaped \uXXXX — json.dumps default differs from canonical_json's
/// ensure_ascii=False!). Backed by Vec<(String, PyVal)> — never a sorted map.
pub struct PyDict(Vec<(String, PyVal)>);
pub fn python_dumps(v: &PyVal) -> String;

pub const PROFITABILITY_SUMMARY_KEY: [&str; 2] = ["revenue", "profitability-summary"];
/// One channel entry: the EXACT 27-key insertion order of lines 702–734
/// ("channel_id", "peer_id", "class", "net_profit_sats", "roi_pct", "days_open",
///  "role", "fee_multiplier", "forward_count", …, "marginal_roi_reliable").
/// class/role/role_30d use enum VALUES (lowercase); roi_pct = py_round(roi_percent, 2);
/// fee_multiplier = py_round(m, 2) (multiplier INJECTED — its computation is fee-stack
/// evidence, Phase 4); open_cost_msat / rebalance_cost_msat = sats * 1000 (the
/// sats→msat dual-column convention: costs are stored sats-native and PROMOTED,
/// revenue is msat-native — never "normalize" one into the other's storage);
/// net_pnl_msat = total_contribution_msat - total_cost_sats*1000 (mixed-unit
/// expression, port verbatim); marginal_roi_reliable is a JSON bool → "true"/"false".
pub fn profitability_summary_payload(entries: &[SummaryEntry], timestamp: i64) -> PyDict;
/// datastore_push envelope decision (transport is 3b wiring): error-key reject,
/// timestamp-if-absent, UTF-8 byte size cap check on the ENCODED string.
pub fn datastore_envelope(payload: PyDict, now: i64, max_bytes: usize) -> Result<String, TelemetryError>;
```

**Fixture generator:** builds 6+ payloads in Python by constructing real `ChannelProfitability` objects (incl. float-artifact ROIs like `10.555000000000001` pre-round, a negative pnl, a >1-channel dict to pin iteration order, non-ASCII alias-free — keys only) and dumps the EXACT `json.dumps` bytes. Rust replays to byte equality (`assert_eq!` on `String`).

- [ ] Failing tests first (fixture bytes + envelope rules: existing timestamp NOT overwritten; `"error"` key → reject; size cap measured on UTF-8 bytes).
- [ ] Implement; commit.

---

### Task 9 (Wave 2, parallel; needs T6): No-double-spend concurrency + restart durability

**Files:** Create `crates/revops-db/tests/budget_concurrency.rs` ONLY.

The gate tests (design spec: NOT compressible):
- [ ] **8-thread contention:** budget 1000, 8 threads × 10 attempts of 300-sat reservations against ONE shared DB file (each thread its own `BudgetDb::open` connection — Python is connection-per-thread). Invariant after the storm: `SUM(active reserved_sats) + windowed spends <= 1000` at EVERY point (assert final state + assert grant count == floor(1000/300) == 3 when no releases).
- [ ] **Cross-category contention:** threads alternate `reserve_spend(category="rebalance")` (via the `reserve_budget` wrapper) and `category="boltz"` against the same unified budget — the DD1/P1-003 invariant: the two categories never jointly overshoot.
- [ ] **Reserve→settle race:** concurrent `mark_spend_reservation_spent(record_event=true)` + new reservations: the spend event lands atomically with the status flip; total committed never dips (no window where a spent reservation is neither reserved nor evented).
- [ ] **Simulated restart (scenario 24 shape):** reserve `restart-1` for 500 → drop the `BudgetDb` (and copy the DB file to a new path to simulate a cold start on WAL — copy db+wal+shm, reopen, and ALSO reopen the original) → `get_spend_reservation_states` shows `restart-1 → ('active', 500)`; a fresh `reserve_spend` against budget 800 for 400 is REFUSED (298+? no: 800−500=300 < 400) — outstanding holds survive restart and still gate.
- [ ] **Crash-window restart:** kill between UPDATE and event write is impossible by construction (single tx) — prove it: open a second connection mid-test holding `BEGIN IMMEDIATE`, assert `mark_spend_reservation_spent` under retry-exhaustion leaves status 'active' after reopen.
- [ ] **WAL dual-writer** (2B obligation (4) analog for the rail): a Python-shaped writer (raw rusqlite doing Python's exact SQL) appends spend_events while Rust reserves — no lost writes, sums agree.
- [ ] All tests tempdir-only; grep-guard test asserts this file contains no absolute `/home` or production path. Commit.

---

### Task 10 (Wave 2, parallel; needs T2,T4,T5,T6): Conformance un-defer flip — ONE task owns conformance.rs

**Files:** Modify `crates/revops-econ/tests/conformance.rs` and `crates/revops-econ/Cargo.toml` ONLY (add `[dev-dependencies]` `revops-analytics = { path = "../revops-analytics" }`, `revops-db = { path = "../revops-db" }`). **Coordination note for the controller:** Phase 4's plan will also touch these two files for ITS flip — sequence the merges; within Phase 3, only this task edits them.

**The flip (9 scenarios DEFERRED → REPLAYED; counts 18→27 / 22→13 in `all_scenarios_replayed_or_deferred`):**

| Scenario | Replay recipe (from the golden drivers; `NOW = 1_752_400_000`) |
|---|---|
| 01 | `classify_channel(0.25, 5000, Some(1752313600), 10, 200, default evidence)` → name == `"PROFITABLE"` |
| 02 | `close_protection_reason` with role_30d=INBOUND_GATEWAY, roi% −10.0, flow(0.9, 50), empty route set → `"INBOUND_GATEWAY"` |
| 04 | `revenue_role_30d(true, 20, 22, OUTBOUND_GATEWAY)` → name `"BALANCED"` (inputs from case.json; lifetime_role parsed from its NAME string) |
| 05 | `marginal_roi` with profit −300 / cost 600 → −0.5 (float expected: compare via the py_repr-aware canonical variant from Phase 2 T8's cycle writer, or `f64::to_bits` vs the parsed fixture float — pick to_bits, document it) |
| 06 | `classify_channel(-0.40, -8000, Some(1740044800), 400, 15, default)` → `"STAGNANT_CANDIDATE"` |
| 07 | evidence: DiagStats{2, 0}, roi −0.40, last_routed NOW−86400*30, days 200, fwd 5 → `"ZOMBIE"` |
| 19 | `policy_close_block(Dynamic, ["protect"])` → `(allowed=false, reason="Channel tagged 'protect' — close blocked")` — byte-compare against the fixture's `—` string |
| 23 | tempdir `BudgetDb`; `reserve_spend(c-1, 800, budget 1000)` → granted; `reserve_spend(c-2, 800, budget 1000)` → refused. Wire `{first_granted, second_granted}` |
| 24 | reserve `restart-1` 500; drop + reopen the SAME tempdir file; `get_spend_reservation_states(["restart-1"])`; expected value is a **Python dict-repr STRING**: `"{'status': 'active', 'reserved_sats': 500}"` — format exactly (single quotes, `", "` between pairs) |

- [ ] Step 1 (failing): flip the 9 out of `DEFERRED` into `REPLAYED` + adjust the two count assertions — build breaks until replays exist.
- [ ] Step 2: implement replay dispatch arms (categories `classification`, plus `authorization`/19 and `reservation`/23–24) using the recipes above; comparison stays `canonical_json(produced) == canonical_json(expected)` except s05 (float — documented exception).
- [ ] Step 3 (deliberate re-tag, log in progress ledger like the conflict-rules flip): scenarios 08–12's DEFERRED owner strings say "Phase 3: fee_stage controller" — that numbering predates the port phase plan; re-tag to `"Phase 4: fee controller (rails/rate-limit/deadband/cooldown/DTS-PID)"` and 13–17 stay Phase 4/5 as written. 19 leaves the list (it was "Phase 3-5"; the pure gate replays now — the *live* close-protection golden suite still rides Phase 6 capacity work). 39 stays deferred (prose-only).
- [ ] Step 4: `cargo test -p revops-econ` green, full workspace green. Commit.

---

### Task 11 (Wave 3, GATE): Phase exit verification

**Files:** optionally `crates/revops-db/tests/budget_prod_copy.rs` (one `#[ignore]`d test).

- [ ] `cargo fmt --check`, `cargo clippy --workspace -- -D warnings`, `cargo test --workspace` green.
- [ ] Conformance: 27 REPLAYED / 13 DEFERRED, `all_scenarios_replayed_or_deferred` green; drift-guard vs port worktree green.
- [ ] All fixture generators committed on the `port` branch worktree; regenerate once and diff == empty (both directions).
- [ ] `#[ignore]`d prod-copy smoke (operator-authorized copy, same precedent as Phase 2's econ_ledger.db replay): copy lnnode's `revenue_ops.db` to a tempdir, `BudgetDb::open` on the COPY, run `get_spend_reservation_states(None)` + the committed-total SELECTs read-only, and cross-check the sums against Python's `get_spend_reservation_states` on the same copy. Copies only; the test hard-fails if pointed at a path containing `revenue_ops.db` outside a tempdir.
- [ ] Update `.superpowers/sdd/progress.md`: phase entry + the 08–12 re-tag note + riders for 3b wiring (analyzer orchestration, telemetry transport via datastore RPC, policy persistence, the `KalmanStep::Untouched` persistence rule).
- [ ] Confirm zero diffs under `crates/revops-fees` (must not exist), `crates/revops-core`, `crates/revops-econ/src`, `crates/revops`, `crates/revops-rpc` — the parallel-safety promise to Phase 4.

## Exit Criteria (Phase 3 gate, from the design spec — NOT compressible)

- [ ] **No double-spend:** 8-thread + cross-category contention and restart-durability suites green (T9); reservation state restores across simulated restart with no double-spend and no freed budget.
- [ ] **Committed-total formula pinned:** P4-017 unfiltered-holds + windowed-spends arithmetic asserted to the sat, both daily and weekly, incl. re-reserve delta and terminal-resurrection refusal (T6).
- [ ] **Frozen telemetry shapes byte-identical:** `["revenue", "profitability-summary"]` payload bytes equal Python `json.dumps` output on generated fixtures (T8).
- [ ] **Phase-owned conformance scenarios flipped and green:** 01, 02, 04, 05, 06, 07, 19, 23, 24 REPLAYED (T10).
- [ ] **Kalman/profitability float parity:** bit-pattern fixture suites green (T3, T7); banker's-rounding boundaries pinned (`py_round` reuse).
- [ ] **No production writes anywhere:** write paths tempdir/own-file only; prod-copy test `#[ignore]`d and copy-guarded.
- [ ] `cargo fmt --check`, `cargo clippy --workspace -- -D warnings`, `cargo test --workspace` green in CI.
