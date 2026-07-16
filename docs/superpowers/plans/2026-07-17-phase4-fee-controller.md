# Phase 4: Fee Controller, Dry-Run First (revops-fees) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Tasks are sized for one implementer subagent each and grouped into parallel waves (see Wave Table) for isolated worktrees per superpowers:using-git-worktrees.

**Goal:** A new `crates/revops-fees` library crate that ports `modules/fee_controller.py` (8,613 LOC — the densest module of the port) plus `modules/admission_policy.py`: discounted Gaussian Thompson sampling (Bayesian quadratic regression with hand-rolled 3x3 Sarrus determinant/inverse and Cholesky draws), the PI inventory controller, Vegas mempool-spike floor, neighbor-gossip market intelligence, chain-cost/rebalance floors, congestion episodes, zero-flow ratchet, drain bias, the htlcmax admission valve, per-channel `v2_state_json` persistence with LOSSLESS legacy-blob round-trip, and the frozen ADR-001 rail order — producing a **dry-run decision journal only** (no `setchannel` broadcast until the explicit fee cutover flag).

**Architecture:** Every algorithm is ported fixture-first: a port-worktree generator runs the ACTUAL Python code over seeded/pinned scenarios and emits input→output vectors that the Rust implementation must match to **exact float equality — bit-for-bit, compared as `py_repr` strings — not epsilon**. Clock and RNG are injected (`now: i64` parameters; a hand-ported CPython `random` module) so every function is a pure fixture target. The cycle orchestrator runs as a single-owner path (the Python `_state_lock`-spanning cycle becomes one owning task), reads the production DB read-only, and appends decisions + full reason traces to a JSONL journal that the new `tools/diff-harness/diff_fee_decisions.py` compares against Python's recorded `fee_changes` traces on lnnode.

**Tech Stack:** Rust workspace crate; deps: `revops-core` (msat, canonical JSON), `revops-econ` (governor/intents/`pyfloat::{py_repr, py_round}` — REUSE, never duplicate), `serde`/`serde_json`, `thiserror`, `rusqlite` (workspace, system sqlite), `sha2` (already workspace). Dev: `tempfile`. **No `rand` crate** — CPython-parity RNG is hand-rolled (Task 1). Python truth: `~/bin/cl_revenue_ops-port` @ branch `port` (v2.18.1).

**Deadline context (2026-07-19 addendum):** This plan is one of the parallel workstreams (spec §Constraints 3). Waves 0–2 are ~1 day of parallel work; Wave 3 is the orchestrator; Wave 4 is the gate + live instruments. Per the addendum, the "N-day dry-run" of the original phase gate compresses to: (a) offline fixture parity (this plan's suites — the floor, not compressible), (b) **htlcmax goldens byte-identical + production `v2_state_json` blobs round-trip losslessly (explicitly on the NOT-compressed floor list)**, (c) an hours-level live dry-run diff window on lnnode via `diff_fee_decisions.py`, then fee cutover.

**Python source of truth:** `/home/sat/bin/cl_revenue_ops-port/modules/fee_controller.py` (all line references below are to this file at branch `port`), `modules/admission_policy.py`, `modules/config.py` (`ChainCostDefaults`), goldens `tests/golden/fixtures/htlcmax/*.json` (12 files) and `tests/golden/fixtures/fee/*.json` (10 files), conformance scenarios 08–13, and `docs/refactor/adr/ADR-001-dts-pid-fee-controller.md` (the frozen rail order). The port map "fees" lens (`docs/port/port-map.json`, lens_maps[1]) enumerates 15 numbered behavioral contracts in its `port_notes` — every one of them is binding on this plan.

## Global Constraints

- `crates/revops-fees/src/lib.rs` root: `#![forbid(unsafe_code)]`; clippy warnings deny in CI; `cargo fmt --check` clean.
- **File-set DISJOINT from Phase 3** (`crates/revops-analytics`, `crates/revops-db` changes): this plan creates `crates/revops-fees` + `fixtures/fees/**` + `fixtures/golden/{htlcmax,fee}/**` + `tools/diff-harness/diff_fee_decisions.py` + port-worktree generators ONLY. `revops-fees` may **depend on** existing `revops-db`/`revops-econ`/`revops-core` APIs but never edits their files. Known cross-phase merge points, each deliberately trivial: (1) root `Cargo.toml` members += `"crates/revops-fees"` (one line, union-merges with Phase 3's member line — Task 1); (2) `crates/revops-econ/tests/conformance.rs` — touched ONLY by Task 12, which is explicitly sequenced after Phase 3's flip task has merged.
- **Clock injection everywhere.** No `SystemTime::now()` inside ported algorithm code — every function that Python computed with `int(time.time())` takes `now: i64`. Fixture generators monkeypatch `time.time` to the pinned `NOW = 1_752_400_000` (same pin as the conformance corpus). Only the Wave-3 orchestrator's entrypoint reads the real clock (once per cycle) and threads it down.
- **RNG injection everywhere.** Sampling functions take `&mut PyRandom` (Task 1). Production seeds from entropy (parity of *distribution*, per ADR-001 term 4's recorded portability hazard); fixtures seed via `CycleContext::derive_seed` values so draws are bit-for-bit reproducible. ADR-001 explicitly allows seed injection ("Future work explicitly allowed").
- **Exact float parity discipline (THE central risk, spec risk register #1):** all float-producing functions are pinned by fixtures whose expected values are CPython `repr(float)` strings; Rust asserts `revops_econ::pyfloat::py_repr(actual) == expected`. No epsilon comparisons anywhere in fixture tests. To keep bit-parity: never reassociate Python arithmetic (port expression shapes and loop accumulation order verbatim), use `f64::{sqrt, ln, cos, sin, powf, log2, ln_1p, log10, floor, ceil, round_ties_even}` (glibc libm on this platform, same as CPython — the pyrand/gauss fixture suite is the canary if this ever diverges), and port Python `int(x)` as truncation toward zero (`x as i64` after finiteness checks match Python's), `round(x)` as `py_round`/`round_ties_even`, `math.ceil/floor` as `f64::ceil/floor` then cast.
- **Every constant is load-bearing, not tunable** (port_notes: "Many constants encode named production incidents"). Constants are transcribed verbatim from the Python class bodies with a pinned-constants test per module (name → exact value). Clamp ORDER is equally load-bearing (e.g. P8-002 stall multiplier applied AFTER the risk-premium max; Vegas decay BEFORE spike check; floor-inversion prefers the ceiling).
- **ADR-001 rail order is FROZEN:** `cooldown(deadband(rate_limit(rails(raw_target))))` with DTS+PID as the authoritative controller. Every rail gets at least one fixture case (Tasks 5/6 vendor + extend the Phase-0 goldens). `_get_market_boundary_fee` must return `None` unconditionally, even with `fee_market_boundary_enabled=true` persisted (behavioral contract 1).
- **Dry-run first:** nothing in this plan calls `setchannel`. The cycle produces `FeeDecision` records (decision + reasons + full algorithm trace) into a JSONL journal. `set_channel_fee`'s logic is ported as a **pure decision function** (clamps, strings, governor gate); the single side-effecting broadcast call is added at cutover, behind the per-subsystem flag, not in this plan.
- Wire strings are frozen: `FeeReasonCode` values (`policy_passive`, `policy_static`, `dts_pid_sample`, `zero_fee_probe`, `zero_fee_probe_success`, `low_fee_exploration`, `low_fee_exploration_success`, `congestion`, `gossip_refresh`, `channel_open`, `skip_sleeping`, `skip_waiting_time`, `skip_waiting_forwards`, `skip_fee_unchanged`), damping `cap_reason` strings (`none`/`normal_cycle_delta_cap`/`wake_cycle_delta_cap`), zero-flow guard tags (`zero_flow_ratchet_guard`/`zero_flow_downshift`/`zero_flow_floor_override`), the governed-failure reason `internal_error ({e})`, and the clamp log format `FEE_LIMIT: Clamped fee for {id[:16]}... from X to Y (limits: A-B PPM)`.
- Strict TDD: each task writes failing tests (fixture-driven where a generator exists) first, then implements. Frequent commits (do not push/PR without controller direction).
- Only Task 1 touches shared crate files (root `Cargo.toml`, `crates/revops-fees/Cargo.toml`, `lib.rs` with ALL modules pre-declared as stubs) → Waves 1–4 never edit shared files → parallel-worktree safe (Phase 2 convention).

## Wave Table (parallelism)

| Wave | Tasks | Parallel-safe? |
|---|---|---|
| 0 | T1 scaffold + pyrand (CPython MT19937/gauss) + mat3 (Sarrus/inverse/Cholesky) + fixture-generator skeleton | sequential gate (everything depends on it) |
| 1 | T2 posterior recompute + discounting · T3 state structs + serde + PI controller + py-JSON writer · T4 admission valve + drain bias (htlcmax goldens) · T5 pure rail stages (fee goldens) | 4-way parallel (disjoint files) |
| 2 | T6 floors/Vegas/congestion evidence rails (needs T5) · T7 Thompson dynamics + sampling (needs T1,T2,T3) · T8 market intelligence (needs T3) | 3-way parallel |
| 3 | T9 v2_state_json lossless round-trip + production blobs [needs-controller/live-data] (needs T3) · T10 dry-run cycle orchestrator + governed authorize + set_channel_fee decision core (needs T4–T9) | T9 ∥ T10 start; T10's final integration test needs T9's envelope |
| 4 | T11 diff harness `diff_fee_decisions.py` (needs T10) · T12 conformance un-defer flip 08–13 (needs T2,T3,T4,T5; **sequenced AFTER Phase 3's conformance flip merges**) | 2-way parallel, T12 last to merge |

Deferred out of this plan (explicit, not silent): actual `setchannel` execution + fee cutover flag wiring in `crates/revops` (cutover step, after the T11 diff window); `record_failed_forward` hook-thread wiring into the plugin binary (the pure nudge function IS ported in T7; the `forward_event` subscription plumbing is Phase 1b/`crates/revops` territory); `set_initial_fee` full RPC path (prior *selection* is T8; the RPC registration is `crates/revops`); scenarios 14–17 (`rebalance_mode` planner — genuinely Phase 5 rebalance-stack scope despite their stale "Phase 4" labels in `DEFERRED`, see Task 12).

---

### Task 1 (Wave 0): Crate scaffold + CPython-parity RNG + 3x3 matrix kernel + fixture generator

**Mirrors:** `fee_controller.py` lines 468–528 (`_mat3_det`, `_mat3_invert`, `_mat3_vec_mul`, `_cholesky3`); CPython `random.py`/`_randommodule.c` (MT19937, `random()`, `gauss()`, `seed(int)`).

**Files:**
- Modify: `Cargo.toml` (root — add `"crates/revops-fees"` to members; all needed deps already in `[workspace.dependencies]` from Phases 1–2: serde, serde_json, thiserror, rusqlite, sha2, hex, tempfile)
- Create: `crates/revops-fees/Cargo.toml`
- Create: `crates/revops-fees/src/lib.rs` — pre-declares ALL modules for the whole phase: `pyrand`, `mat3`, `thompson` (dir module: `thompson/mod.rs`, `thompson/recompute.rs`, `thompson/dynamics.rs`, `thompson/sampling.rs`, `thompson/serde.rs`), `pid`, `vegas`, `admission`, `drain`, `profiles`, `rails`, `floors`, `market`, `state_store`, `pyjson`, `cycle`, `journal`, `execution`, `reason` — stub files with doc comments only
- Create: `crates/revops-fees/src/pyrand.rs`, `src/mat3.rs`
- Create (port worktree, branch `port`): `/home/sat/bin/cl_revenue_ops-port/tools/port/gen_fees_fixtures.py` — the ONE generator all later tasks extend (subcommands per suite; every suite pins `NOW = 1_752_400_000` and formats every float as `repr(f)`)
- Create: `fixtures/fees/pyrand/*.json`, `fixtures/fees/mat3/*.json` (generated then committed here)
- Test: `crates/revops-fees/tests/pyrand.rs`, `tests/mat3.rs`

```toml
# crates/revops-fees/Cargo.toml
[package]
name = "revops-fees"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
revops-core = { path = "../revops-core" }
revops-econ = { path = "../revops-econ" }
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
rusqlite.workspace = true

[dev-dependencies]
tempfile.workspace = true
```

**Interfaces (produced for every later task):**

```rust
// mat3.rs — verbatim expression shapes from fee_controller.py:468-528.
pub type M3 = [[f64; 3]; 3];
pub type V3 = [f64; 3];

/// Sarrus/cofactor 3x3 determinant (py lines 468-472). EXACT expression shape:
pub fn det3(m: &M3) -> f64 {
    m[0][0] * (m[1][1] * m[2][2] - m[1][2] * m[2][1])
        - m[0][1] * (m[1][0] * m[2][2] - m[1][2] * m[2][0])
        + m[0][2] * (m[1][0] * m[2][1] - m[1][1] * m[2][0])
}

/// Cofactor-transpose inverse (py 474-501). None when
/// |det| < 1e-10 * max(1.0, max_elem*max_elem*max_elem)  — RELATIVE tolerance,
/// cube written as three multiplications exactly as Python.
pub fn invert3(m: &M3) -> Option<M3>;

pub fn matvec3(m: &M3, v: &V3) -> V3;   // py 503-510, exact a*b + c*d + e*f shape

/// Lower-triangular Cholesky (py 512-528). Returns None when a diagonal
/// pivot (m[i][i] - s) < 1e-12, or when dividing by L[j][j] < 1e-12.
/// s accumulates k = 0..j in ascending order (Python sum() order).
pub fn cholesky3(m: &M3) -> Option<M3>;
```

```rust
// pyrand.rs — CPython random module, exact.
pub struct PyRandom { /* mt: [u32; 624], index: usize, gauss_next: Option<f64> */ }
impl PyRandom {
    /// CPython random.seed(n) for a non-negative int: init_by_array over the
    /// int's little-endian 32-bit words (n == 0 => key [0]).
    pub fn seed_from_u64(n: u64) -> Self;
    /// genrand_res53: (a>>5)*67108864.0 + (b>>6)) * (1.0/9007199254740992.0)
    pub fn random(&mut self) -> f64;
    /// CPython random.gauss (Box–Muller with cached second value):
    ///   if let Some(z) = self.gauss_next.take() { return mu + z * sigma; }
    ///   let x2pi = self.random() * std::f64::consts::TAU;
    ///   let g2rad = (-2.0 * (1.0 - self.random()).ln()).sqrt();
    ///   let z = x2pi.cos() * g2rad;
    ///   self.gauss_next = Some(x2pi.sin() * g2rad);
    ///   mu + z * sigma
    pub fn gauss(&mut self, mu: f64, sigma: f64) -> f64;
}
```

- [ ] **Step 1 (failing tests):** `tests/pyrand.rs` loads `fixtures/fees/pyrand/sequences.json` (not yet generated — write the generator first in the same step, commit fixtures alongside): for seeds `[0, 1, 42, 2**31-1, 4319832543551537554]` (last one = `derive_seed(0, "fee-sample")` — compute it via `revops_econ::context`; ERRATUM fixed 2026-07-16: the plan originally quoted 8544829456559915396, which is wrong — T1 derives the value definitionally and the real value is 4319832543551537554), pin the first 16 `random()` outputs and first 16 `gauss(0,1)` outputs as `repr` strings, plus an interleaving case (`random, gauss, gauss, random, gauss`) to pin the `gauss_next` cache across call patterns. `tests/mat3.rs` loads `fixtures/fees/mat3/{det,invert,cholesky,matvec}.json`: ≥20 vectors each including identity, the DTS default prior `[[0.01,0,0],[0,0.01,0],[0,0,0.01]]`, realistic post-fit precision matrices (generate by running the actual Python `_recompute_posterior_core` on 3 seeded observation sets and dumping `posterior_precision`), a singular matrix (expect `null`), a near-singular matrix just above/below the relative tolerance (branch pin both sides), and a non-PD matrix for `cholesky3 -> null`. Expected values are `repr` strings; Rust compares `py_repr(x)`.
- [ ] **Step 2:** write `tools/port/gen_fees_fixtures.py` in the port worktree with subcommands `pyrand`, `mat3` (later tasks add more). It imports `modules.fee_controller` directly and calls the real static methods. Commit it on branch `port`; copy JSON outputs into `fixtures/fees/` here.
- [ ] **Step 3:** implement `pyrand.rs` (MT19937 hand-rolled: 624-word state, init_by_array seeding, tempering; ~80 lines) and `mat3.rs`. Run tests → all fixture vectors bit-identical.
- [ ] **Step 4:** `cargo test -p revops-fees`, `cargo clippy --workspace -- -D warnings` green. Commit.

---

### Task 2 (Wave 1, parallel): Posterior recompute core + legacy fallback + zero-regime anchor + DTS discounting

**Mirrors:** `fee_controller.py` `_recompute_posterior` (1296–1305), `_recompute_posterior_core` (1307–1576), `_recompute_posterior_legacy` (1578–1630), `apply_dts_discount` (1672–1719), `MIN_PRECISION = 0.000025`, `DISCOUNT_WEIGHT_FLOOR = 0.05`, plus the read-only helpers it needs: `_positive_revenue_mass` (860–892), `_earning_region_fee` (894–900), `_effective_positive_rate_ref` (851–858).

**Files:** Create `crates/revops-fees/src/thompson/recompute.rs` (replacing stub), extend `tools/port/gen_fees_fixtures.py` (subcommands `posterior`, `discount`), create `fixtures/fees/posterior/*.json`, `fixtures/fees/discount/*.json`. Test: `crates/revops-fees/tests/posterior.rs`.

**Interfaces:**

```rust
// Operates on the GaussianThompsonState struct from thompson/mod.rs (Task 3
// defines the struct + serde; Task 1's stub pre-declares the module so both
// tasks compile independently — recompute.rs takes &mut GtsCore, the pure
// numeric field bundle, defined HERE and re-exported by thompson/mod.rs).
pub struct Observation {
    pub fee: f64, pub revenue_rate: f64, pub weight: f64, pub ts: i64,
    pub time_bucket: String,
    pub flag: Option<String>,        // 6th tuple element: "zero_probe" | "congestion"
    pub extra: Vec<serde_json::Value>, // elements beyond 6 — preserved verbatim (lossless)
}
pub fn recompute_posterior(state: &mut GaussianThompsonState, now: i64);       // core + bias re-apply
pub fn recompute_posterior_core(state: &mut GaussianThompsonState, now: i64);  // py 1307-1576 verbatim
pub fn recompute_posterior_legacy(state: &mut GaussianThompsonState,
    weighted_obs: Option<&[(f64, f64, f64)]>, now: i64);                       // py 1578-1630
pub fn apply_dts_discount(state: &mut GaussianThompsonState, gamma: f64);      // py 1672-1719
pub fn positive_revenue_mass(state: &GaussianThompsonState, now: i64) -> Vec<(f64, f64)>;
pub fn earning_region_fee(state: &GaussianThompsonState, now: i64) -> Option<f64>;
pub fn effective_positive_rate_ref(state: &GaussianThompsonState, now: i64) -> f64;
```

**The discounting order-of-operations, stated explicitly from the Python (transcribe as a doc comment — divergence here silently changes every draw):**
1. `apply_dts_discount(gamma)` does three things IN PLACE, in this order: (a) Gaussian: `precision = 1/max(std^2, 1.0)`, `precision *= gamma`, floor at `MIN_PRECISION = 0.000025`, `std = sqrt(1/precision)`; (b) polynomial: every cell of `posterior_precision *= gamma`; (c) persistent forgetting: every stored observation's base weight `w := max(min(w, 0.05), w * gamma)` — never raises a weight already below the floor, never lets discounting push one below it.
2. The NEXT `_recompute_posterior_core` rebuilds `Ln` from the **fixed prior** `_prior_precision` + the (now decayed) observation weights and OVERWRITES `posterior_precision`/`posterior_std` — so (a) and (b) only affect samples drawn between the discount and the next recompute, while (c) is the durable channel. The controller's per-channel cycle order is: `update_posterior(...)` (which ends in a recompute) → `apply_dts_discount(gamma)` → `sample_fee_contextual(...)`. The sample therefore draws from the discounted-in-place matrices. Do not "optimize" by recomputing after the discount.
3. Inside `_recompute_posterior_core`, the weighted-obs collection order is the stored observation order; the regression accumulation is `for obs: for i in 0..3 { rhs[i] += wi*phi[i]*rev; for j in 0..3 { Ln[i][j] += wi*phi[i]*phi[j] } }` with `f = (fee - fee_min) * inv_range`, `inv_range = 1.0 / fee_range`, `wi = w * inv_sigma2`, `sigma2 = max(10.0, noise_variance)`. Preserve exactly — reassociation flips the singularity branch on near-singular fits.
4. Noise-variance update AFTER solving: `new_sigma2 = ss / max(sw - 3.0, 1.0)`; `noise_variance = max(10.0, 0.7*new_sigma2 + 0.3*noise_variance)`.
5. Branch tree of `_recompute_posterior_core` to port verbatim (each branch gets fixtures): empty observations → prior reset + `charged_fee_mean = 0.0`; weight cutoff `< 1e-6` skips an obs (but it still joined `anchor_pool` only if ≥ cutoff — read py 1335–1351 carefully: the cutoff excludes from BOTH, `anchor_pool.append` happens after the cutoff and before the zero-probe skip); `charged_fee_mean` only overwritten when `total_w > 0` (stale value retained otherwise); zero-mass / streak-override anchor paths (earning-anchor variant with `spread_std=(max-min)/4`, `max_std = sqrt(1/MIN_PRECISION)`, `ZERO_REGIME_REL_STD = 0.15`, and the 24h `ZERO_REGIME_ANCHOR_HALF_LIFE_HOURS` re-weighting with `ts >= zero_run_start_ts` filtering under streak override) — both set `_last_fee_min = _last_fee_max = 0.0` to disable polynomial sampling; `< 3` obs or `fee_range < 5.0` → legacy; singular `Ln` → legacy; concave (`a < -1e-8`) → `f_star = clamp(-b/(2a), -0.5, 1.5)` un-normalized + delta-method std (`da = b/(2a*a)`, `db = -1/(2a)`, `var = Σ grad_i Σ_ij grad_j`, `std = max(MIN_STD, sqrt(max(0,var)) * fee_range)`); non-concave → log-1.1 bucket LCB selection (`key = int(ln(max(fee,1.0))/ln(1.1))` — `int()` truncates toward zero; `n_eff = bw²/Σw²`; winner by `mean - sqrt(var)/sqrt(max(n_eff,1))`, mean-fee of winning bucket) + spread-std inflated by `sqrt(len/max(total_w,1e-6))` clamped `[MIN_STD, sqrt(1/MIN_PRECISION)]`.

- [ ] **Step 1 (failing tests):** extend the generator with `posterior`: ~25 scenarios, each `{observations (explicit 5/6-tuples with timestamps relative to NOW), prior fields, zero_revenue_streak/zero_run_start_*, expected: {posterior_mean, posterior_std, posterior_coeffs[3], posterior_precision[3][3], noise_variance, charged_fee_mean, _last_fee_min, _last_fee_max} as repr strings}` — cover every branch in item 5 above, including: exactly-3-obs minimal fit, 200-obs full buffer, all-zero-revenue anchor, streak-override-with-earning-history anchor, probe-heavy anchor pool, singular fit → legacy, narrow-range → legacy, concave and non-concave (whale-window winsorization case for `positive_revenue_mass`: ≥4 masses, cap at 3x median). Add `discount`: sequences of `(recompute, discount γ∈{0.98, 0.992, 0.95}, discount again, recompute)` pinning field values after EACH step (order-of-operations proof). Also pin `positive_revenue_mass`/`earning_region_fee`/`effective_positive_rate_ref` outputs directly.
- [ ] **Step 2:** run generator in port worktree (monkeypatching `time.time`), copy fixtures, watch tests fail.
- [ ] **Step 3:** implement; constants transcribed into `thompson/mod.rs`-owned `impl` consts (this task writes them into `recompute.rs` as `pub(crate) const` if Task 3 hasn't landed; Task 3's struct move is a pure re-export — coordinate via the pre-declared module layout so neither edits the other's file).
- [ ] **Step 4:** all fixture vectors bit-identical; pinned-constants test (`MIN_STD=10`, `DECAY_HOURS=168.0`, `MIN_PRECISION=0.000025`, `DISCOUNT_WEIGHT_FLOOR=0.05`, `SUPPORTED_CEILING_*`, `ZERO_*` family — full list from py 258–383). Clippy green. Commit.

---

### Task 3 (Wave 1, parallel): State structs + lossless serde + PI controller + Python-JSON writer

**Mirrors:** `GaussianThompsonState` field set + `to_dict`/`from_dict` (py 384–462, 1721–1940), `PIDState` (1964–2043), `ChannelFeeState` (2052–2223), `ChannelCycleState` (2226–2325), `FeeAdjustment` (2404–2435), `FeeProfileSettings` (2438–2467), `FeeReasonCode` (195–229), `_PID_TARGET_RATIOS` (1953–1961).

**Files:** Create `crates/revops-fees/src/thompson/mod.rs` (struct + consts + re-exports), `src/thompson/serde.rs`, `src/pid.rs`, `src/reason.rs`, `src/pyjson.rs`. Test: `crates/revops-fees/tests/state_serde.rs`, `tests/pid.rs`. Extend generator (`pid`, `state_dict` subcommands) + `fixtures/fees/pid/*.json`, `fixtures/fees/state_dict/*.json`.

**Interfaces + the tricky contracts in full:**

```rust
// thompson/mod.rs
pub struct GaussianThompsonState {
    pub prior_mean_fee: f64, pub prior_std_fee: f64,        // ints in Python defaults (200/100) but arithmetic is float — store f64, serialize preserving original JSON number type via serde_json::Number passthrough on round-trip (see serde.rs)
    pub observations: Vec<Observation>,
    pub posterior_mean: f64, pub posterior_std: f64,
    pub posterior_coeffs: V3, pub posterior_precision: M3, pub noise_variance: f64,
    pub prior_coeffs: V3, pub prior_precision: M3,          // Python _prior_*
    pub last_fee_min: f64, pub last_fee_max: f64,           // Python _last_fee_*
    pub contextual_posteriors: indexmap-like ordered map<String, CtxPosterior>, // see pyjson note
    pub posterior_bias: Vec<(f64, f64, i64)>,
    pub charged_fee_mean: f64,
    pub zero_revenue_streak: i64, pub zero_run_start_fee: f64, pub zero_run_start_ts: i64,
    pub positive_rate_ref: f64, pub positive_rate_ref_ts: i64,
    pub meaningful_gap_ema_hours: f64, pub last_meaningful_ts: i64,
    pub last_upward_probe_ts: i64,
    pub exploration_boost: f64,                             // retired; blob-compat only
    pub last_sampled_fee: i64, pub last_sample_time: i64,
    pub reseeded_at: i64,                                   // retired; blob-compat only
    pub extra: serde_json::Map<String, serde_json::Value>,  // UNKNOWN top-level keys, preserved
}
/// 4-tuple (mean, precision, count, last_update); legacy 3-tuple (mean, std,
/// count) converted ON LOAD exactly as from_dict does (precision = 1/max(std²,
/// MIN_STD²), last_update 0). Both sampler and updater must ALSO accept the
/// 3-tuple at runtime (sample_fee_contextual/update_contextual re-check len).
pub struct CtxPosterior { pub mean: f64, pub precision: f64, pub count: i64, pub last_update: i64,
                          pub was_legacy_3tuple: bool }

// serde.rs — to_dict/from_dict as data transforms on serde_json::Value:
pub fn gts_to_dict(s: &GaussianThompsonState) -> serde_json::Value;   // EXACT key order of py to_dict (1721-1756), incl. derived "posterior_variance" = float(std)**2 and "weight_scheme": "exposure_v2"
pub fn gts_from_dict(d: &serde_json::Value) -> GaussianThompsonState; // py 1758-1940 verbatim: legacy weight rescale when weight_scheme != "exposure_v2" (w / min(1, log1p(rate)/log1p(1000)) for rate>0 [only when factor>0 and w>0], w / 0.15 for zero-rate, then min(1.0, w)); 4-tuple obs get "normal" bucket appended; coeff/matrix shape+positive-diagonal validation with defaults; bias entries validated/bounded to last 50; every scalar's TypeError/ValueError → default fallback mirrored by serde try-parse
```

```rust
// pid.rs
pub struct PidState { pub kp: f64, pub ki: f64, pub kd: f64, pub ewma_error: f64,
    pub integral_error: f64, pub prev_ewma_error: f64, pub last_update_time: i64,
    pub integral_clamp: f64 }
/// py 1977-2020 verbatim: dt = 0 on first update else max((now-last)/3600, 0);
/// target from _PID_TARGET_RATIOS (default 0.5); non-finite ratio → target;
/// ewma = 0.3*raw + 0.7*ewma; scale = 1/log2(max(cap,1)/1_000_000 + 2);
/// integral += ewma*dt clamped ±integral_clamp ONLY when dt > 0;
/// multiplier = clamp(1.5^(p+i), 0.5, 2.0). prev_ewma_error updated; d_term 0.
pub fn calculate_multiplier(s: &mut PidState, current_outbound_ratio: f64,
                            capacity_sats: i64, flow_state: &str, now: i64) -> f64;
pub fn pid_to_dict(s: &PidState) -> serde_json::Value;   // key order of py 2022-2030
pub fn pid_from_dict(d: &serde_json::Value) -> PidState;
```

```rust
// pyjson.rs — Python json.dumps parity writer. v2_state_json blobs are written
// by Python `json.dumps(v2_data)` with the DEFAULT separators (", ", ": ") —
// i.e. ", " between items and ": " after keys (NOT the compact (",", ":")
// canonical form revops-core uses). Pin this with a generator fixture rather
// than trusting memory: the fixture is a Python-dumped dict with nested
// containers, and the Rust writer must reproduce it byte-for-byte.
pub fn dumps_python(v: &serde_json::Value) -> String;
// Rules: key order = insertion order (use serde_json with the "preserve_order"
// feature — add `features = ["preserve_order"]` on the WORKSPACE serde_json
// only if it is not already set; if changing the workspace dep is off-limits
// (it is — shared file), implement an ordered Value wrapper locally instead:
// pyjson::OValue with Vec<(String, OValue)> maps. Floats via
// revops_econ::pyfloat::py_repr; ints as digits; true/false/null lowercase;
// strings with Python's ensure_ascii=True default (\uXXXX escapes for
// non-ASCII — pin with a fixture containing a non-ASCII alias string).
```

**Decision (locked in):** implement `pyjson::OValue` locally (order-preserving tree + parser) — do NOT touch the workspace serde_json feature set (shared-file ban, and `preserve_order` would silently change `canonical_json` input ordering elsewhere). `state_store`/serde tasks parse blobs into `OValue`, convert typed fields out of it, and keep unparsed subtrees verbatim for lossless re-emission.

- [ ] **Step 1 (failing tests):** generator `pid`: 12 sequences of `calculate_multiplier` calls (varying dt including first-call dt=0, NaN ratio, capacity {1, 1e6, 5e6, 5e8}, all flow states incl. unknown string) pinning `(multiplier, ewma_error, integral_error)` per step as repr strings — scenario 12's `pid_multiplier = 1.026338203439` (rounded 12) must fall out of the same code path. Generator `state_dict`: build Python states (incl. one with 6-tuple congestion + zero_probe observations, one legacy `weight_scheme`-absent blob with outcome-scaled weights, one with 3-tuple contextual posteriors, one with unknown injected keys `{"future_field": {"x": 1}}`), dump `json.dumps(state.to_dict())` bytes; Rust asserts (a) `dumps_python(gts_to_dict(gts_from_dict(parse(blob))))` — for the CURRENT-format blob — is byte-identical to Python's `json.dumps(GaussianThompsonState.from_dict(d).to_dict())` output (generator emits both sides), (b) legacy-blob load produces the exact rescaled weights (repr-pinned), (c) unknown keys survive load→dump (content-identical position: unknown keys re-emitted — pin exact bytes from the generator, which runs the same load→dump through Python for truth).
- [ ] **Step 2:** implement `reason.rs` (enum with `as_str`, count-pinned test = 14 variants), `profiles` CONSTANTS LIVE IN Task 5 (do not define here). Implement structs, serde, pid, pyjson.
- [ ] **Step 3:** fixtures bit-identical; commit.

---

### Task 4 (Wave 1, parallel): Admission valve (htlcmax) + drain-bias helpers + vendored goldens

**Mirrors:** `modules/admission_policy.py` (whole file, 69 lines) + `fee_controller.py` shims 2878–2918 + drain helpers 89–192 (`compute_node_receivable_ratio`, `node_drain_pressure`, `_cfg_bool`, `_cfg_float`, `effective_drain_discount_max`) and `_drain_fee_multiplier` (3069–3087).

**Files:** Create `crates/revops-fees/src/admission.rs`, `src/drain.rs`; Create `fixtures/golden/htlcmax/` — **byte-for-byte copy** of `~/bin/cl_revenue_ops-port/tests/golden/fixtures/htlcmax/` (12 files, `cp -r`, vendored like the conformance corpus). Test: `crates/revops-fees/tests/admission.rs`, `tests/drain.rs`.

**Interfaces:**

```rust
// admission.rs — pure functions of (cfg-slice, channel_info, flow_state).
pub const DEPLETION_SPENDABLE_FRACTION: f64 = 0.85;
pub const FLOOR_MSAT: i64 = 10_000_000;
pub const UPDATE_DEADBAND_FRAC: f64 = 0.10;

pub struct HtlcmaxCfg {
    /// Python accepts bool True OR strings "true"/"1"/"yes" (lowercased);
    /// anything else (incl. truthy ints) is DISABLED. Model as an enum
    /// carrying the raw config value and port the exact test.
    pub enable_dynamic_htlcmax: serde_json::Value,
    pub htlcmax_source_pct: f64, pub htlcmax_sink_pct: f64, pub htlcmax_balanced_pct: f64,
}
/// None when disabled or capacity<=0. target = int(capacity_msat * pct) —
/// f64 multiply then trunc-toward-zero cast, NOT integer math (parity with
/// Python int()). Then min(target, int(spendable_msat*0.85)), then
/// max(FLOOR_MSAT, min(target, capacity_msat)).
pub fn compute_htlcmax_msat(cfg: &HtlcmaxCfg, capacity_sats: i64,
                            spendable_msat: i64, flow_state: &str) -> Option<i64>;
/// equal → false; current<=0 → true; else |new-current| > current * 0.10
/// (f64 compare exactly as Python: int > int*float promotes to f64).
pub fn delta_exceeds_deadband(new_msat: i64, current_msat: i64) -> bool;

// drain.rs
pub fn compute_node_receivable_ratio(channels: &[NodeChannel]) -> f64;  // py 89-116: CHANNELD_NORMAL only; no capacity → 1.0
pub fn node_drain_pressure(receivable_ratio: f64, target: f64, floor: f64) -> f64; // py 118-138 incl. target<=floor degenerate guard
pub fn effective_drain_discount_max(static_max: f64, bias_enabled: bool,
                                    bias_max: f64, node_pressure: f64) -> f64; // py 167-192: bias contributes ONLY when enabled is actual bool true; result >= static_max always
pub fn drain_fee_multiplier(local_ratio: f64, forward_count: i64,
                            high_liquidity_threshold: f64, discount_max: f64) -> f64; // py 3069-3087
```

- [ ] **Step 1 (failing tests):** a table-driven test iterating ALL 12 vendored golden files: parse `{inputs: {cfg_overrides, channel_info, flow_state}, htlcmax_msat}`, apply defaults (`enable=true, source .85, sink .25, balanced .50` — from `test_golden_htlcmax.py::_cfg`) overlaid with `cfg_overrides`, assert exact `htlcmax_msat` (or null → None). Byte-identity guard test: when `~/bin/cl_revenue_ops-port` exists (or `REVOPS_PY_CORPUS` set), every vendored file's bytes equal the source's. Deadband: the 4 golden deadband cases + boundary case `|delta| == current*0.10` exactly (strict `>` — must be false). Drain tests: transcribe `default-off invariant` (bias disabled ⇒ result identical to static max for all pressures) + ramp/degenerate cases from py docstrings; pin with 10 generator vectors (`drain` subcommand) as repr strings.
- [ ] **Step 2:** implement; commit.

---

### Task 5 (Wave 1, parallel): Pure rail stages — profiles, step cap, blend, damping, exploration target, zero-flow ratchet + vendored fee goldens

**Mirrors:** `FEE_PROFILES` tables (2522–2548), `EXPLORATION_*` consts (2549–2552), `_resolve_fee_profile`/`get_fee_profile_settings` (3049–3068), `_get_fee_step_cap` (5124–5143), `_is_sparse_data_channel` (5145–5161), `_get_target_blend_ratio` (5163–5206), `_blend_fee_target` (5208–5235), `_get_exploration_fee_target` (5237–5278), `_apply_damped_fee_target` (5280–5313), `_zero_flow_streak_thresholds` (5315–5343), `_apply_zero_flow_ratchet_guard` (5345–5439), `_is_unroutable_zero_window` (5441–5457), `ZERO_FLOW_*`/`UNROUTABLE_SPENDABLE_SATS` consts (grep the class body), `_exploration_std_threshold` (2732–2776), `_kalman_demand_factor` (2708–2731).

**Files:** Create `crates/revops-fees/src/profiles.rs`, `src/rails.rs`; Create `fixtures/golden/fee/` — byte-for-byte copy of `tests/golden/fixtures/fee/` (10 files). Test: `crates/revops-fees/tests/rails.rs`. Extend generator (`rails` subcommand) → `fixtures/fees/rails/*.json`.

**Interfaces (the rail pipeline signature — the frozen ADR-001 seam):**

```rust
// profiles.rs
pub struct FeeProfileSettings { pub min_observation_hours: f64, pub min_forwards_for_signal: i64,
    pub dts_discount_gamma: f64, pub dts_sparse_discount_gamma: f64,
    pub normal_target_blend_ratio: f64, pub wake_target_blend_ratio: f64,
    pub sparse_target_blend_ratio: f64, pub normal_cycle_max_delta_ratio: f64,
    pub normal_cycle_min_delta_ppm: i64, pub wake_cycle_max_delta_ratio: f64,
    pub wake_cycle_min_delta_ppm: i64 }
pub fn fee_profile(name: &str) -> (&'static str, &'static FeeProfileSettings); // unknown → "active" (mirror _resolve_fee_profile fallback exactly)
// "conservative" table verbatim: (1.0, 6, 0.992, 0.996, 0.20, 0.10, 0.10, 0.25, 25, 0.10, 10).
// "active" table transcribed from the class constants it references — pin BOTH tables with a generator fixture (`rails` emits profile.to_dict() for each).

// rails.rs
pub struct DampingDiag { pub requested_delta_ppm: i64, pub max_delta_ppm: i64,
    pub cap_reason: &'static str, pub cap_applied: bool, pub wake_damping_applied: bool }
pub struct BlendDiag { pub blend_ratio: f64, pub blended_delta_ppm: i64,
    pub sparse_data_conservative: bool }

pub fn fee_step_cap(current_fee_ppm: i64, woke_from_sleep: bool, profile: &FeeProfileSettings) -> i64;
// = max(min_delta, ceil(max(current,1) as f64 * ratio) as i64)  — math.ceil of a float product

pub fn target_blend_ratio(woke: bool, sparse: bool, posterior_std: f64,
                          profile_name: &str, profile: &FeeProfileSettings) -> f64;
pub fn blend_fee_target(current: i64, bounded_target: i64, woke: bool, sparse: bool,
                        posterior_std: f64, profile_name: &str, profile: &FeeProfileSettings)
                        -> (i64, BlendDiag);
// blended_delta = round_ties_even(requested_delta as f64 * ratio) as i64 (Python int(round(...)))
// with the ±1 minimum-step rule when requested != 0 but blended == 0.

/// ADR-001 stage 2 (rate_limit) + stage 4 (cooldown wake-variant). The
/// conformance 09/10/11 cases replay THIS function directly.
pub fn apply_damped_fee_target(current: i64, target: i64, woke: bool,
                               profile: &FeeProfileSettings) -> (i64, DampingDiag);

pub fn exploration_fee_target(current: i64, floor_ppm: i64, cfg_min_fee_ppm: i64,
                              sparse: bool, effective_min_fee_ppm: Option<i64>) -> i64;

pub fn zero_flow_streak_thresholds(gap_ema_hours: f64, cycle_hours: f64) -> (i64, i64);
pub struct ZeroFlowInputs { pub current_fee: i64, pub target_fee: i64, pub min_fee: i64,
    pub zero_revenue_streak: i64, pub forwards_since_update: i64, pub revenue_rate: f64,
    pub supported_fee_ceiling: Option<f64>, pub earning_anchor_ppm: Option<f64>,
    pub guard_streak: Option<i64>, pub downshift_streak: Option<i64>,
    pub rate_is_meaningful: Option<bool> }
pub fn apply_zero_flow_ratchet_guard(i: &ZeroFlowInputs) -> (i64, Option<&'static str>);
// Tags frozen: "zero_flow_ratchet_guard" / "zero_flow_downshift" / "zero_flow_floor_override";
// downshift fires only when (streak - downshift_thresh) % ZERO_FLOW_DOWNSHIFT_INTERVAL_CYCLES == 0;
// downshift_cap = floor(current * ZERO_FLOW_DOWNSHIFT_RATIO), min'd with int(supported_cap) when finite>0;
// soft anchor floor min(current, int(anchor * ZERO_FLOW_ANCHOR_FLOOR_FRAC)); tag-honesty rules verbatim (py 5398-5439).

pub fn is_unroutable_zero_window(revenue_rate: f64, spendable_sats: f64) -> bool;
pub fn kalman_demand_factor(expected_demand: f64) -> f64;      // clamp(ed/0.5, 1.0, 2.0) — may only HALVE an observation
pub fn exploration_std_threshold(current_fee_ppm: i64) -> f64; // py 2732-2776; strict '>' users, see contract 9
```

- [ ] **Step 1 (failing tests):** replay the 6 vendored `damping_*.json` goldens through `apply_damped_fee_target` (inputs `{current, target, woke}` → `{applied_fee_ppm, diag{...}}`, exact field equality incl. `cap_reason` strings — scenario 09/10/11 expected blocks are the same data). Generator `rails` vectors for: step cap boundary (`current=1`, ratio rounding via ceil), blend ratio band edges (std exactly 200.0/100.0/50.0 — `>=` boundaries), ±1 blend minimum step both directions, exploration target (floor-pinned channel returns exploration_floor; sparse halving; `ceil(exploration_floor*1.25)` vs headroom candidate vs discounted ceiling interplay — 8 vectors as repr/int pins), zero-flow guard: all three tags + hold-vs-downshift interval arithmetic (streak = thresh, thresh+11, thresh+12 with interval 12) + trickle reclassification (`rate_is_meaningful=false, rate>0` → silence) + floor-override raise, thresholds cadence-scaling (gap capped at `ZERO_FLOW_GAP_CAP_HOURS`, `downshift = max(downshift, guard)`), kalman factor + std threshold curves (10 points each).
- [ ] **Step 2:** implement; pinned-constants test for every `ZERO_FLOW_*`, `EXPLORATION_*`, `UNROUTABLE_SPENDABLE_SATS`, `GOSSIP_GATE_SUPPRESSION_RATIO` and both profile tables. Byte-identity guard for the vendored `fee/` goldens (same pattern as Task 4). Commit.

---

### Task 6 (Wave 2, parallel): Evidence-backed floors — chain-cost floor, rebalance floor, flow ceiling, congestion detect, Vegas reflex, class-aware min fee

**Mirrors:** `_calculate_floor` (8130–8251), `ChainCostDefaults` (in `modules/config.py` — transcribe the constants + `calculate_floor_ppm` verbatim into this crate; do NOT wait for the config workstream), `_get_dynamic_chain_costs_live` (8253–8311), `_get_rebalance_cost_floor` (4069–4159), `_get_flow_adjusted_ceiling` (4161–4223), `_detect_congestion` (5459–5502), `VegasReflexState` (2328–2401), `_effective_min_fee_ppm` (2811–2877), `_is_flow_balanced_router`/`_get_flow_window_map` (2777–2810).

**Files:** Create `crates/revops-fees/src/floors.rs`, `src/vegas.rs`. Test: `crates/revops-fees/tests/floors.rs`, `tests/vegas.rs`. Extend generator (`floors`, `vegas`) → `fixtures/fees/floors/*.json`, `fixtures/fees/vegas/*.json`.

**Interfaces:**

```rust
// floors.rs — pure over explicit evidence inputs (the cycle passes data; no RPC/DB here).
pub struct ChainCosts { pub open_cost_sats: i64, pub close_cost_sats: i64, pub sat_per_vbyte: f64 }
pub struct PeerLatency { pub avg: f64, pub std: f64 }

/// ADR-001 stage 1 (rails: floor). Conformance 08 replays this
/// (capacity 2_000_000, chain_costs None, opener "local" → 21).
/// Formula frozen: floor = max(base_floor, risk_premium) * stall_multiplier
/// with the stall markup applied AFTER the max (P8-002); risk premium =
/// (sat_per_vbyte * 150 * 0.001 / 50_000) * 1e6; remote opener excludes
/// open cost; result max(1, int(floor)).
pub fn calculate_floor(capacity_sats: i64, chain_costs: Option<&ChainCosts>,
                       peer_latency: Option<&PeerLatency>, opener: &str) -> i64;

pub fn dynamic_chain_costs(perkb: &serde_json::Value) -> Option<ChainCosts>;
// opening|mutual_close|unilateral_close|floor|1000 fallback chain; /1000 to sat/vB;
// open = clamp(int(svb*140), 500, 50000); close = clamp(int(svb*200), 300, 50000).

pub struct RebalanceCostSample { pub cost_sats: i64, pub amount_sats: i64, pub timestamp: i64 }
pub fn rebalance_cost_floor(flow_state: &str, recent_costs: &[RebalanceCostSample],
                            peer_fallback: Option<(&str /*confidence*/, i64 /*avg_fee_ppm*/)>,
                            now: i64) -> Option<i64>;
// sink/dormant → None; >=4 samples in 30d: cost_ppm = (Σcost*1e6) // Σvolume (INTEGER floor div),
// floor = int(cost_ppm as f64 * 1.20); fallback needs confidence in {"medium","high"} and >0.
// Cap cost_ppm at 5000 where the Python caller does (verify: the cap lives at the
// call site/get_channel_cost_history contract — transcribe from the actual code path).

pub fn flow_adjusted_ceiling(current_fee: i64, base_ceiling: i64,
                             last_forward_ts: Option<i64>, now: i64) -> i64;
pub fn detect_congestion(flow_state_row: Option<&FlowStateRow>, live_htlc: Option<&LiveHtlc>,
                         htlc_congestion_threshold: f64, flow_interval: i64, now: i64) -> bool;
pub fn effective_min_fee_ppm(/* class-aware: saturated (outbound>=0.85) or source
    channels take min(min_fee_ppm, min_fee_ppm_saturated) when saturated_cfg <
    min_fee_ppm, UNLESS flow-balanced router (7d |out-in|/(out+in) <= 0.33 AND
    turnover >= 0.25x capacity) — transcribe py 2777-2877 */) -> i64;

// vegas.rs
pub struct VegasReflexState { pub intensity: f64, pub decay_rate: f64,
    pub last_sat_vb: f64, pub last_update: i64, pub consecutive_spikes: i64 }
/// Decay FIRST, then spike check (documented invariant). >=4x → intensity 1.0;
/// 2x..4x → boost=(ratio-2)/2; trigger when consecutive>=2 OR rng.random() < boost*0.5,
/// adding min(1.0, intensity + boost*0.3). ma<=0 → 1.0 guard.
pub fn vegas_update(s: &mut VegasReflexState, current_sat_vb: f64, ma_sat_vb: f64,
                    rng: &mut PyRandom, now: i64);
pub fn vegas_floor_multiplier(s: &VegasReflexState) -> f64; // <0.01 → 1.0 else 1 + sqrt(i)*2
```

- [ ] **Step 1 (failing tests):** replay all 4 vendored `floor_*.json` goldens (`{capacity_sats, chain_costs, opener} → floor_ppm`; `floor_live_chain_costs_local`/`floor_remote_opener_cheaper` exercise the ChainCosts path; scenario 08's `floor_ppm: 21` must reproduce). Generator `floors` vectors: stall-multiplier-after-max pin (case where risk premium wins AND stall applies — the P8-002 regression), risk-premium arithmetic repr pins, rebalance floor integer-division pin (`(total_cost*1_000_000)//total_volume` — Rust `i64` floor division of positives), fallback confidence gating, flow ceiling day boundaries (2.99/3/7 days), congestion live-vs-snapshot resolution incl. the 2x-flow_interval staleness TTL and missing-timestamp-is-fresh rule. Generator `vegas`: seeded `PyRandom` sequences through 12 update cycles (spike patterns crossing 2x/4x, decay-only cycles) pinning `(intensity, consecutive_spikes, floor_multiplier)` repr strings per cycle — this pins decay-before-check AND the probabilistic branch RNG consumption order (one `random()` call happens ONLY in the 2x..4x branch when `consecutive_spikes < 2` — Python short-circuits `or`; consuming the draw unconditionally desyncs the stream: TRAP, port the short-circuit).
- [ ] **Step 2:** implement (incl. `ChainCostDefaults` constants transcribed from `modules/config.py` with a pinned-constants test). Commit.

---

### Task 7 (Wave 2, parallel): Thompson dynamics + sampling paths

**Mirrors:** `update_posterior` (725–833), `is_meaningful_rate` (835–849), `supported_fee_ceiling` (902–940), `maybe_upward_probe_cap`/`consume_upward_probe` (942–978), `_time_similarity` (980–1004), `update_contextual` (1006–1107), `_update_related_time_contexts` (1109–1168), `record_posterior_nudge` (1170–1224), `_blend_posterior_toward` (1226–1244), `_posterior_bias_shift` (1246–1273), `_apply_posterior_bias` (1275–1294), `real_observation_count` (530–540), `sample_fee` (542–602), `sample_fee_contextual` (604–669), `_sample_from_polynomial_posterior` (671–723), `apply_vegas_adjustment` (1636–1659), `get_exploitation_fee` (1632–1634), failed-forward nudge math from `record_failed_forward` (8527–8613, the pure part: `is_fee_relevant_failure` + weight `0.1 * min(3.0, 1 + log10(amount_sats)/3)` + 0.8x target).

**Files:** Create `crates/revops-fees/src/thompson/dynamics.rs`, `src/thompson/sampling.rs`. Test: `crates/revops-fees/tests/thompson_dynamics.rs`, `tests/thompson_sampling.rs`. Extend generator (`update`, `sampling`, `ceiling`) → `fixtures/fees/{update,sampling,ceiling}/*.json`.

**Interfaces + the posterior-update contract in full:**

```rust
// dynamics.rs
/// py 725-833 verbatim ORDER: (1) input guards (bad hours→1.0; bad/neg rate→0.0;
/// bad fee→return, NO state change); (2) weight = min(1.0, hours/6.0);
/// (3) meaningful = rate > 0 && rate >= 0.10 * effective_positive_rate_ref(now);
/// (4) streak bookkeeping (meaningful: reset streak + EMA updates incl.
/// positive_rate_ref seeding/EMA-on-DECAYED-ref and gap-EMA; else: stamp
/// zero_run_start on streak==0, streak += 1); (5) append obs (6-tuple with
/// "congestion" flag when congested); (6) probe injection when !meaningful &&
/// streak >= 4 && zero_run_start_fee > 0 && posterior_mean >= 0.3 * (earning
/// anchor OR zero_run_start_fee): probe_fee = max(1, int(fee * 0.9)), only if
/// probe_fee < fee, appended as ("zero_probe", rev 0.0, SAME weight/ts/bucket);
/// (7) prune to last 200; (8) recompute_posterior(state, now).
pub fn update_posterior(state: &mut GaussianThompsonState, fee: f64, revenue_rate: f64,
                        hours: f64, time_bucket: &str, congested: bool, now: i64);
pub fn is_meaningful_rate(state: &GaussianThompsonState, revenue_rate: f64, now: i64) -> bool;
pub fn supported_fee_ceiling(state: &GaussianThompsonState, now: i64,
                             floor_ppm: Option<f64>) -> Option<f64>; // 0.90 mass quantile x 1.25; floor-escape 2.0x when quantile_fee <= floor
pub fn maybe_upward_probe_cap(state: &GaussianThompsonState, now: i64, supported_cap: f64) -> Option<f64>;
pub fn consume_upward_probe(state: &mut GaussianThompsonState, now: i64);
pub fn update_contextual(state: &mut GaussianThompsonState, context_key: &str,
                         fee: f64, revenue_rate: f64, time_bucket: &str, now: i64);
// incl. hierarchical-prior init (role "S" widens by 1.25), 7-day precision decay from
// last_update, CTX_PRECISION_DECAY 0.98 per update, min precision 1/200², time/revenue/role
// weights, cross-pollination of adjacent buckets at 0.1x (count NOT incremented), prune to
// 104 most-used when > 130 (sort by count DESC — Python sorted() is stable: ties keep
// insertion order, which the OValue-ordered map preserves. TRAP: a BTreeMap here changes
// which contexts survive the prune).
pub fn record_posterior_nudge(state: &mut GaussianThompsonState, target_fee: f64,
                              weight: f64, now: i64); // 5% dedup-refresh (max weight), cap 50, immediate mean-only blend w/(1+w)
pub fn posterior_bias_shift(state: &GaussianThompsonState, base: f64, now: i64) -> f64; // sequential decayed blends, skip < 1e-3
pub fn apply_vegas_adjustment(state: &mut GaussianThompsonState, vegas_multiplier: f64,
                              new_floor: f64, now: i64); // >1.2 gate; std boost min(mult,2.0); nudge(new_floor, 0.43)
pub fn failed_forward_nudge_weight(amount_sats: f64) -> f64;
pub fn is_fee_relevant_failure(failcode: Option<i64>, failreason: Option<&str>) -> bool;

// sampling.rs — all draws through &mut PyRandom.
pub fn sample_fee(state: &mut GaussianThompsonState, floor: i64, ceiling: i64,
                  exploration_multiplier: Option<f64>, rng: &mut PyRandom, now: i64) -> i64;
pub fn sample_fee_contextual(state: &mut GaussianThompsonState, context_key: &str,
                             floor: i64, ceiling: i64, exploration_multiplier: Option<f64>,
                             rng: &mut PyRandom, now: i64) -> i64;
fn sample_from_polynomial_posterior(state: &GaussianThompsonState, floor: i64, ceiling: i64,
                                    noise_scale: f64, rng: &mut PyRandom) -> Option<f64>;
```

**RNG stream discipline (port EXACTLY — the fixture suite enforces it):** `sample_fee` consumes: sparse path → ONE `gauss` (prior draw, std `max(10, prior_std*1.1)*boost`); else polynomial path → `invert3`+`cholesky3` succeed: THREE `gauss(0,1)` (each scaled by `noise_scale` BEFORE `matvec3(L, z)`); Cholesky fails: THREE `gauss(0,1)` (diagonal approximation `sqrt(max(1e-6, Σ[i][i]))`); polynomial returns non-concave/None: falls THROUGH to ONE more `gauss` (Gaussian posterior draw). The bias shift applies on the prior path and the polynomial-success path but NOT the Gaussian-fallback path (posterior_mean already carries nudges). `last_sampled_fee`/`last_sample_time` stamped with the injected `now` (Python stamps `int(time.time())` — inject).

- [ ] **Step 1 (failing tests):** generator `update`: 20 multi-step scenarios (sequences of `update_posterior` calls with pinned timestamps stepping NOW forward) pinning full state after each step (obs tuples, streaks, refs, EMA hours, posterior fields — repr strings): trickle-extends-streak, probe injection start/stop (mean falls below 0.3x anchor), congestion-flag exclusion from `supported_fee_ceiling` but INCLUSION in `real_observation_count`, probe exclusion from both, EMA-on-decayed-ref pin, gap-EMA pin, prune at 201 obs. Generator `ceiling`: `supported_fee_ceiling` incl. floor-escape and winsorization; `maybe_upward_probe_cap` gate matrix (streak≠0 / mean≤cap / std<60 / within 24h). Generator `sampling`: seeded scenarios (seed = `derive_seed` values) pinning the SAMPLED FEE bit-for-bit for: sparse-prior path (with a live nudge → shift applied), polynomial concave path, Cholesky-fallback path (construct a precision matrix whose inverse is non-PD), non-concave→Gaussian-fallback path, contextual offset path (4-tuple and legacy 3-tuple ctx, `ctx_count < 5` passthrough, cap `±0.20*|base|`, confidence `n/(n+10)`), exploration_multiplier clamping `[0.75, 2.0]` + non-finite fallback 1.0. Nudge tests: dedup-refresh within 5% tolerance (`max(existing, new)` weight), 50-cap eviction, decay-prune at `< 1e-3`, and the "recompute erases then bias re-applies" round: `update_posterior` → assert bias survived (repr pin).
- [ ] **Step 2:** implement. **Do not reorder guard checks** — e.g. `maybe_upward_probe_cap` checks cap-parse → finite/positive → streak → mean → std → interval, in that order. Commit.

---

### Task 8 (Wave 2, parallel): Market intelligence (neighbor gossip)

**Mirrors:** `_get_network_fee_prior_live` (3179–3231), `_gossip_cache_ttl_seconds` (3253–3270), `_get_peer_inbound_channels_live` (3271–3305), `_get_market_boundary_fee` (3306–3323, **stub: always None**), `_get_neighbor_fee_median_live` (3324–3405), `_is_cln_default_fee` (3406–3428), `_get_neighbor_fee_percentile_live` (3429–3483), `_get_competitive_undercut_pct` (3484–3553), `_get_channel_rebalance_cost_ppm` (3554–3596), `_get_context_with_values` (3597–3638), `_select_best_fee_prior` (7924–7949), `_frozen_observation` (3134–3141) and the cycle-frozen wrappers (3142–3178).

**Files:** Create `crates/revops-fees/src/market.rs`. Test: `crates/revops-fees/tests/market.rs`. Extend generator (`market`) → `fixtures/fees/market/*.json`.

**Interfaces:** pure functions over an explicit gossip snapshot (`&[GossipChannel]` with `{fee_ppm, base_fee_msat, capacity_sats, last_update_ts, source, destination}`), plus a `FrozenObservations` memo struct (one `HashMap<String, serde_json::Value>` per cycle — compute-once semantics; the Wave-3 cycle owns one instance so every channel in a cycle sees identical gossip values, mirroring `_frozen_observation`):

```rust
pub fn neighbor_fee_median(peer_channels: &[GossipChannel], our_id: &str, now: i64) -> Option<i64>;
// weight = capacity_millions * 1/age_days; fee range filter 1..=10000; CLN-default
// (base 1000 msat AND ppm 10) excluded; >= 3 competitors required; weighted median.
pub fn neighbor_fee_percentile(peer_channels: &[GossipChannel], our_id: &str, pct: f64, now: i64) -> Option<i64>;
pub fn competitive_undercut_pct(capacity_rank: usize, rank_count: usize, neighbor_median: i64,
                                invert_rank: bool) -> f64;
// 5%-15% base by rank, +5% if median > 300, halved if median < 100, clamp [0.03, 0.20].
pub fn market_boundary_fee(_cfg_enabled: bool) -> Option<i64> { None } // ALWAYS None (contract 1) — test pins enabled=true → None
pub fn select_best_fee_prior(candidates: &[FeePrior]) -> Option<FeePrior>;
pub const INITIAL_PRIOR_NUDGE_WEIGHT: f64 = 0.3;
pub struct GossipCache { /* TTL from cfg (default 1800s), 500-entry eviction,
    snapshot-iterate on eviction (written pre-lock by design) */ }
```

- [ ] **Step 1 (failing tests):** generator `market`: median/percentile vectors from synthetic gossip sets (repr/int pins) covering: exactly-3-competitor boundary (2 → None), CLN-default exclusion, age weighting (fresh vs 30-day-old), range filter, weighted-median tie behavior (transcribe the exact index selection from the Python), undercut pct across ranks {0, mid, last} × median {50, 200, 500} × invert_rank, boundary stub (enabled → None), prior selection order.
- [ ] **Step 2:** implement + TTL/eviction unit tests (no wall clock — injected now). Commit.

---

### Task 9 (Wave 3, parallel): v2_state_json lossless round-trip + production state blobs **[needs-controller / live data]**

**Mirrors:** `_load_persisted_fee_strategy_row` (3639–3656), `_extract_fee_state_payload` (3658–3689), `_extract_cycle_state_payload` (3691–3715), `_serialize_cycle_state_payload` (3717–3741), `_build_merged_fee_strategy_row` (3743–3873), `_build_fee_strategy_row_kwargs`/`_serialize_fee_strategy_row`/`_persist_fee_strategy_row`/`_flush_pending_fee_strategy_rows` (3963–4058), `ChannelFeeState.to_v2_dict`/`from_v2_dict` (2138–2223), the explicit-shared-fields tracking (2105–2136, 2294–2325), and `_get_cycle_state`/`_save_cycle_state` (8313–8411).

**Files:** Create `crates/revops-fees/src/state_store.rs`. Test: `crates/revops-fees/tests/state_roundtrip.rs` (+ `#[ignore]`d `tests/production_blobs.rs`). Extend generator (`v2_blob`) → `fixtures/fees/state_roundtrip/*.json`.

**Interfaces:**

```rust
/// LOSSLESS envelope: every level parses known fields into types and keeps
/// EVERYTHING else (unknown keys, order, int-vs-float number types) in the
/// pyjson::OValue tree it was parsed from. Round-trip contract:
///   dumps_python(parse(blob).to_ovalue()) is CONTENT-identical to blob
///   (same key/value multiset at every level, int/float distinction intact,
///   floats re-emitted via py_repr), and for blobs Python itself would
///   re-emit unchanged, BYTE-identical.
pub struct V2StateEnvelope {
    pub algorithm_version: String,      // "thompson_aimd_v1" and "dts_pid_v1" both known (2180)
    pub fee_state: FeeStatePayload,     // nested-first; flat top-level fallback for pre-nesting rows (3661-3679)
    pub cycle_state: CycleStatePayload, // may be absent (legacy rows): defaults from scalar columns (3694-3715)
    pub shared: SharedScalars,          // last_gossip_refresh, last_broadcast_at, dynamic_htlcmin_baseline_msat (flat canonical copies)
    pub raw: pyjson::OValue,            // the FULL parsed tree — source of truth for re-emission
}
pub fn parse_v2_blob(blob: &str, row: &FeeStrategyRow) -> V2StateEnvelope;
pub fn load_fee_state(env: &V2StateEnvelope, row: &FeeStrategyRow) -> ChannelFeeState;   // from_v2_dict semantics incl. unknown-version migration stamp "dts_pid_v1"
pub fn load_cycle_state(env: &V2StateEnvelope, row: &FeeStrategyRow) -> ChannelCycleState;
/// Merged write shape (3833-3856): {"algorithm_version", "fee_state", "cycle_state",
/// three flat shared scalars} — flat thompson_state/pid_state mirrors are NOT
/// re-written (deliberate WAL-churn fix); explicit-shared-field merge rules verbatim
/// (caller-authoritative only when explicitly assigned; fee-caller preference when
/// fee_state given without cycle_state, else cycle preference).
pub fn build_merged_row(channel_id: &str, cycle: Option<&ChannelCycleState>,
                        fee: Option<&ChannelFeeState>, persisted: &V2StateEnvelope)
                        -> (FeeStrategyRow, pyjson::OValue);
pub struct PendingRows { /* last-write-wins per channel; serialized EXACTLY ONCE at flush */ }
pub fn read_fee_strategy_rows(conn: &rusqlite::Connection) -> Vec<FeeStrategyRow>; // read-only; production schema incl. all migration columns
```

- [ ] **Step 1 (failing tests, synthetic):** generator `v2_blob` builds 10 representative blobs THROUGH the actual Python classes: current nested-layout blob, pre-nesting flat blob (thompson_state at top level), `thompson_aimd_v1` legacy blob, 5-tuple-only observations, 6-tuple `zero_probe`/`congestion` observations, 3-tuple contextual posteriors, missing `pid_state`, missing `cycle_state`, injected unknown keys at THREE levels (top, `fee_state`, `thompson_state`), and one blob with non-ASCII in `last_context_key`. For each: (a) Rust parse→re-emit content-identical (and byte-identical where the generator's Python load→save round also produced identical bytes — the generator emits both `input_blob` and `python_roundtrip_blob`; Rust must match `python_roundtrip_blob` byte-for-byte after a load→save through the typed structs); (b) `load_fee_state` field values pinned (repr strings); (c) explicit-shared-field merge matrix (9 cases: {explicit, warm-default, absent} × {fee-caller, cycle-caller, both}) matching generator truth.
- [ ] **Step 2 (production blobs — REQUIRES CONTROLLER: read-only fetch from lnnode):** the implementer does NOT ssh; request the snapshot from the controller with exactly this command and record the request in the task report:

```bash
# controller runs (read-only; path from: ssh lnnode 'lightning-cli listconfigs revenue-ops-db-path')
ssh lnnode 'sqlite3 -readonly "$REVOPS_DB_PATH" \
  "SELECT channel_id, last_revenue_rate, last_fee_ppm, trend_direction, step_ppm,
          consecutive_same_direction, last_update, last_broadcast_fee_ppm,
          is_sleeping, sleep_until, stable_cycles, forward_count_since_update,
          last_volume_sats, last_state, v2_state_json
   FROM fee_strategy_state;" -json' > fixtures/fees/production/fee_strategy_state.json
```

  Committed under `fixtures/fees/production/` (operator data on the operator's own repo — acceptable; ~50KB × channel count). `tests/production_blobs.rs` (`#[ignore]`, run explicitly in the gate): for EVERY production row, (a) parse succeeds with zero unknown-format warnings OR each divergence is enumerated in the test output; (b) parse→re-emit is content-identical (assert exact multiset equality via OValue comparison — number type intact); (c) parse→`load_fee_state`→`to_v2_dict` path→re-emit is byte-identical to Python performing the same load→save (generator subcommand `v2_prod_check` runs the Python side over the same JSON export in the port worktree and emits expected bytes). **This is the "production state blobs round-trip losslessly" gate item from the design spec — NOT compressible.**
- [ ] **Step 3:** implement `PendingRows` flush semantics (last-write-wins, serialize-once, per-row fallback order preserved) with unit tests. Commit.

---

### Task 10 (Wave 3): Dry-run cycle orchestrator + governed authorization + set_channel_fee decision core

**Mirrors:** `_adjust_channel_fee` (5504–7355) — the stage sequence pinned below; `adjust_all_fees`/`_adjust_all_fees_inner`/`_adjust_all_fees_channel_loop` (4413–4884), `_prefetch_neighbor_gossip` (4491–4531), `_classify_no_adjustment_skip_reason` (4885–4922), `_should_force_gossip_refresh`/`_create_gossip_refresh_adjustment` (4923–5086), `wake_all_sleeping_channels`/`_maybe_wake_for_vegas_spike` (4295–4412), `_fee_governor_enabled`/`_governed_authorize_fee_broadcast` (7521–7626), `set_channel_fee` (7627–7923) as a pure decision, `_handle_policy_change` semantics (7356–7402), `get_dts_summary` (5087–5123), `_set_last_decision_summary` (3031–3048).

**Files:** Create `crates/revops-fees/src/cycle.rs`, `src/journal.rs`, `src/execution.rs`. Test: `crates/revops-fees/tests/cycle.rs`. Extend generator (`cycle`) → `fixtures/fees/cycle/*.json`.

**The per-channel stage sequence (frozen; transcribed from `_adjust_channel_fee` — every numbered stage below carries a line anchor so the implementer ports against the code, not this summary):**
1. Profile resolve; congestion detect (5579); sleep/wake handling (5617–5710: force-reprice wake, timer-expiry wake, revenue-spike wake with `last_rate<=0 → any new rate wakes`, congestion wake L4) — **ADR stage 4 cooldown**, plus observation-window gates (min hours / min forwards → `skip_waiting_time`/`skip_waiting_forwards`).
2. Observation ingest: demand-adjusted rate via `kalman_demand_factor`, unroutable-window censoring (5824 spendable check → obs AND streak skipped), `update_posterior(..., congested=prev_congestion_active)` (5965–6012: the observation describes the PREVIOUS window — flag from the episode state BEFORE this cycle's transition), `update_contextual`, then `apply_dts_discount(gamma = sparse ? profile.dts_sparse_discount_gamma : profile.dts_discount_gamma)` (6373).
3. Floors/ceiling (**ADR stage 1 rails**): `effective_min_fee_ppm` → `base_floor = calculate_floor(...)` (5854) → flow-state multiplier (source 1.10 / sink 0.75, 5833–5835) → Vegas multiplier (5861–5864) → rebalance floor max (5870) → `floor_ppm = max(...)`; ceiling = `flow_adjusted_ceiling(current, max_fee_ppm)` (5905–5908) then policy `fee_multiplier_min/max` anchoring (5916–5933); floor-inversion guard: ceiling wins, floor lowered to `max(effective_min_fee, ceiling-10)`; if still inverted, `ceiling = floor + 10` (5937–5955).
4. Priority chain: **congestion** episode (5970–6097: entry-edge undamped jump to `min(ceiling, max(2x, +250ppm))` cap; episode cap 4x entry fee; later cycles damped; 2 quiet cycles to exit via `congestion_quiet_cycles`) → **bounded low-fee exploration** (6099–6174, `is_under_exploration` with its own posterior update path) → **DTS+PID** (6373–6465): `dts_fee = sample_fee_contextual(context_key, floor, ceiling)`; `pid_multiplier = calculate_multiplier(...)`; `post_pid = int(dts_fee * pid_mult)`; drain multiplier (6434–6442); neighbor-median advisory nudges + undercut clamps (6480–6697: strict `>` vs `exploration_std_threshold`, outbound_ratio ≥ 0.35 gate, `record_posterior_nudge(median, 0.15)` / `(undercut, 0.10)`); supported-fee ceiling cap + upward-probe stretch (6721–6763; `consume_upward_probe` fires ONLY after the applied fee actually crosses the pre-stretch cap, 7231).
5. `bounded = clamp(post_pid, floor, ceiling)` (6765) → pending-target anchor pull (6786–6803: blend FROM `pending_target_ppm` when it survived the 5% gate and clamps into bounds; anchor cleared when crossed) → `blend_fee_target` (6805) → zero-flow ratchet guard (6841–6857, with cadence-scaled thresholds and `rate_is_meaningful`) → `apply_damped_fee_target` (**ADR stage 2 rate_limit**, 6863).
6. htlcmax valve (6907) — piggybacks on any broadcast; a standalone htlcmax move needs `delta_exceeds_deadband`.
7. Gossip gate (**ADR stage 3 deadband**, 7019–7161): suppression band `GOSSIP_GATE_SUPPRESSION_RATIO` vs `last_broadcast_fee_ppm`; suppressed target persisted to `cycle.pending_target_ppm` (6974, 7039); `pending_target_ppm = 0` on reach/broadcast (7161, 7236); sleep bookkeeping (3 stable cycles → sleep); gossip-refresh +1 ppm nudge for 24h-frozen channels.
8. Decision emit: **dry-run** — instead of `self.set_channel_fee(...)` (7203), the orchestrator calls `execution::decide_set_channel_fee(...)` and appends the `FeeDecision` to the journal. NO RPC side effects.

```rust
// cycle.rs
pub struct CycleDeps<'a> {           // injected evidence; NO hidden clock/RNG/IO
    pub evidence: &'a dyn FeeEvidence,    // trait: channels_info, flow states, chain costs,
                                          // gossip snapshot, rebalance cost history, peer latency,
                                          // volume_since, last_forward_time, policies, mempool MA —
                                          // impl over read-only production DB + revops-rpc snapshots
    pub cfg: &'a FeeCfgSnapshot,
    pub rng: &'a mut PyRandom,
    pub now: i64,
}
pub fn run_fee_cycle(state: &mut ControllerState, deps: &mut CycleDeps<'_>) -> Vec<FeeDecision>;
pub fn adjust_channel_fee(/* one channel; returns Option<FeeDecision> + trace */) -> Option<FeeDecision>;

// journal.rs — JSONL, one line per decision, dumps_python-serialized:
pub struct FeeDecision {
    pub channel_id: String, pub peer_id: String,
    pub old_fee_ppm: i64, pub new_fee_ppm: i64,
    pub reason: String, pub reason_code: String,          // FeeAdjustment wire shape (2426-2435)
    pub algorithm_values: pyjson::OValue,                 // full trace: raw_dts_target_ppm, pid_multiplier,
        // post_pid/bounded/blended/applied targets, floor/ceiling terms (base, vegas_mult,
        // rebalance_floor, effective_min), blend/damping diags, zero_flow tag, supported cap,
        // congestion episode fields, htlcmax_msat, gossip-gate disposition, skip reason —
        // superset of Python's algorithm_values + last_decision_summary (3031-3046)
    pub would_broadcast: bool,                            // dry-run: what set_channel_fee WOULD have done
    pub governed: Option<GovernedTrace>,                  // decision + reason_code + intent id/key
    pub cycle_id: String, pub at: i64,
}
pub struct Journal { /* append-only JSONL at <rust-db-dir>/fee_dryrun_journal.jsonl */ }

// execution.rs — set_channel_fee as a PURE decision (no RPC in this plan):
pub struct SetFeeRequest { pub channel_id: String, pub fee_ppm: i64, pub enforce_limits: bool,
    pub effective_min_fee_ppm: Option<i64>, pub htlcmax_msat: Option<i64>, pub base_fee_msat: i64 }
pub struct SetFeeDecision { pub success: bool, pub clamped_fee_ppm: i64, pub message: String,
    pub clamp_log: Option<String> }
/// ABS clamp [0, 100000] ALWAYS; economic clamp [econ_min, max_fee_ppm] unless
/// enforce_limits == false; effective_min_fee_ppm may only LOWER the min term
/// (never raise, never exceed global min_fee_ppm); PASSIVE skips, STATIC pins;
/// clamp log string byte-exact (Global Constraints).
pub fn decide_set_channel_fee(req: &SetFeeRequest, cfg: &FeeCfgSnapshot,
                              policy: Option<&PeerPolicy>) -> SetFeeDecision;

/// Governed gate via revops-econ (REUSE — no re-implementation):
/// zero-cost SET_FEE intent (expires now+600, priority 50, budget_bucket "fees",
/// origin_policy "fee_controller_governed", reversible true, snapshot_id
/// "fee-broadcast-{now}"), authorized through GovernorFacade with reserve_spend
/// stubbed always-true; paused/authority("fees")/staleness gates; intent_proposed
/// ledger event with rendered explanation (ROUTE FLOATS THROUGH THE py_repr-AWARE
/// RENDERER — Phase 2 carry-obligation: Explanation::render panics on floats).
/// FAILS CLOSED: any Err → (false, format!("internal_error ({e})")).
pub fn governed_authorize_fee_broadcast(...) -> (bool, String, Option<GovernedTrace>);
```

- [ ] **Step 1 (failing tests):** generator `cycle`: 15 single-channel scenarios run through the REAL Python `_adjust_channel_fee` with seeded `random`, monkeypatched `time.time`, and a scripted plugin/database/data_service test double (reuse the fakes from `tests/test_fee_controller*.py` — copy the double into the generator, do not import test files): sleeping-hold, timer-wake, spike-wake, congestion entry-edge → damped follow-up → 2-quiet-exit, exploration path, plain DTS+PID with undercut clamp, zero-flow hold + downshift, floor-inversion, gossip-gate suppress → pending-anchor blend next cycle, PASSIVE and STATIC policies, gossip-refresh nudge. Pin per scenario: the full `FeeAdjustment.to_dict()` (or skip reason) + selected state fields after the cycle — the Rust test replays the same evidence through `adjust_channel_fee` with the same seed and compares the decision dict CONTENT-identically (floats via py_repr) and the sampled fee bit-for-bit.
- [ ] **Step 2:** implement `cycle.rs` as a single-owner path (one `&mut ControllerState`, no locks — the plugin binary later wraps it in an actor task per the spec's threading translation rule; document this in the module docs). Implement journal + execution + governed gate (unit tests: fail-closed `internal_error (boom)` string; zero-cost path emits NO `budget_reserved` event — reuse Phase 2's governor test pattern).
- [ ] **Step 3:** integration test: 3-channel synthetic cycle end-to-end → journal lines parse, `would_broadcast` consistent with gossip-gate disposition, state store received one flush batch (serialize-once assertion). Commit.

---

### Task 11 (Wave 4, parallel): Live diff instrument — `diff_fee_decisions.py`

**Files:** Create `tools/diff-harness/diff_fee_decisions.py` (follows the Phase-1 diff-harness conventions in `tools/diff-harness/` — same arg style, same PASS/FAIL/SKIP report shape, name-mapping awareness `revenue-r-*` ↔ `revenue-*`).

**Contract:** compares the Rust dry-run journal against Python's recorded decisions on lnnode over a shared time window:
- Inputs: `--journal <path to fee_dryrun_journal.jsonl>`, `--python-db <path>` (read `fee_changes` rows + reasons via `sqlite3 -readonly`; also `revenue-fee-debug`/last-decision-summary via `lightning-cli` when `--live`), `--since <ts>`, `--tolerance-ppm` (default 0 for deterministic fields).
- **Comparison semantics (sampling is unseeded in production — exact fee equality is NOT the criterion):** per (channel, cycle-window) pair, compare EXACTLY: reason_code; skip classification; floor/ceiling terms (base floor, vegas multiplier bucket, rebalance floor presence, effective_min); damping diag (`cap_reason`, `max_delta_ppm`); zero-flow guard tag; gossip-gate disposition (suppressed/broadcast + pending anchor value); congestion episode fields; htlcmax_msat; policy handling (STATIC pin value, PASSIVE skip); governed decision + reason_code. Compare STATISTICALLY (report-only, no hard fail): sampled `new_fee_ppm` must lie within `[floor_ppm, ceiling_ppm]` of the Python trace and within the per-cycle delta cap of the shared `old_fee_ppm` — flag outliers.
- Output: per-field mismatch table + summary counts; exit non-zero on any deterministic-field mismatch.
- [ ] **Step 1:** self-test mode (`--self-test`): synthetic journal + synthetic fee_changes rows covering match, deterministic mismatch, statistical outlier, and window-alignment (the 120s clustering rule from `econ_reconcile.fee_intent_completeness` — reuse that tolerance for cycle grouping).
- [ ] **Step 2:** implement; document the lnnode run command in the file header (controller executes the live window; the implementer only ships the tool + self-test). Commit.

---

### Task 12 (Wave 4, LAST TO MERGE): Conformance un-defer flip — scenarios 08–13

**⚠ Sequencing (explicit dependency):** this task edits `crates/revops-econ/tests/conformance.rs`, which Phase 3's own flip task also edits (scenarios 01–07, 19, 23, 24). **Do not start this task until the controller confirms Phase 3's conformance flip has MERGED to main**; then branch from post-merge main so the edit is conflict-free.

**Ownership audit (resolves the DEFERRED list's stale labels — the current labels use the Python refactor's phase numbering, not this port's spec phases):**
- Scenarios **08–12** (`fee_stage`: rails / rate_limit / deadband / cooldown / DTS+PID components) are labeled "Phase 3: fee_stage controller" in `DEFERRED`, but the spec's Phase 3 (revops-analytics/revops-db) contains no fee controller — these are **genuinely phase-4-owned** (this plan implements every function they replay: T6 `calculate_floor`, T5 `apply_damped_fee_target`, T2/T3 posterior+PID).
- Scenario **13** (`admission`: dynamic-htlcmax) — labeled "Phase 4", **genuinely phase-4-owned** (T4).
- Scenarios **14–17** (`rebalance_mode` planner) — labeled "Phase 4" but they replay planner/priority logic that lives in the rebalance stack: **genuinely Phase 5**. This task RELABELS them to `"Phase 5: rebalance stack (planner)"` without replaying them.

**Files:** Modify `crates/revops-econ/tests/conformance.rs` ONLY (+ `crates/revops-econ/Cargo.toml` `[dev-dependencies]` — see below).

- [ ] **Step 1:** add `revops-fees = { path = "../revops-fees" }` to `crates/revops-econ/Cargo.toml` `[dev-dependencies]` (dev-dep cycles through the package are permitted by Cargo; verify with `cargo test -p revops-econ` FIRST — if the workspace rejects it, fall back plan: create `crates/revops-fees/tests/conformance_fees.rs` replaying 08–13 from the same `fixtures/conformance/scenarios` root, and change the `DEFERRED` entries' labels to `"replayed in crates/revops-fees/tests/conformance_fees.rs"` so the every-scenario-accounted test stays truthful; record which route was taken in the progress ledger).
- [ ] **Step 2 (failing first):** remove 08–13 from `DEFERRED` → `all_scenarios_replayed_or_deferred` fails. Add replay arms:
  - **08** `fee-rail-floor`: `revops_fees::floors::calculate_floor(inputs.capacity_sats, None, None, "local")` → `{"floor_ppm": 21}`.
  - **09/10/11** rate-limit/deadband/cooldown: `revops_fees::rails::apply_damped_fee_target(inputs.current, inputs.target, inputs.woke, fee_profile("active").1)` → compare `{applied_fee_ppm, diag{...}}` — diag serialized with the exact key set of the Python dict.
  - **12** `dts-pid-components`: fresh `GaussianThompsonState`; `update_posterior` for each `dts_observations` entry (`time_bucket "normal"`, `congested false`, now = NOW); fresh `PidState` + `calculate_multiplier(0.2, 5_000_000, "balanced", NOW)` with `last_update_time = 0` (dt=0 per the case's `fresh_state`/notes). Expected floats were generated with Python `round(x, 12)` (see `tools/conformance/generate_scenarios.py:226-229`): compare `py_repr(py_round(actual, 12)) == py_repr(expected_as_f64)` — read the generator lines and transcribe its exact rounding before writing the comparison.
  - **13** `dynamic-htlcmax`: `revops_fees::admission::compute_htlcmax_msat(defaults + cfg_overrides, ...)` → `{"htlcmax_msat": 1000000000}`.
- [ ] **Step 3:** relabel 14–17 to `"Phase 5: rebalance stack (planner)"` (and 16 keeps its Boltz note). Full `cargo test --workspace` + clippy green. Commit. Record the flip + the ownership-relabel rationale in the progress ledger entry for this task.

---

## Exit Criteria (Phase 4 gate — items 1–3 are on the spec's NOT-compressible floor)

- [ ] **htlcmax golden fixtures byte-identical:** all 12 vendored goldens replay exactly (T4) and the vendored bytes match the Python repo's (guard test).
- [ ] **Production `v2_state_json` blobs round-trip losslessly:** every `fee_strategy_state` row from the lnnode snapshot passes parse→re-emit content-identity AND the Python-load-save byte-identity check (T9 `#[ignore]` test executed and recorded in the ledger).
- [ ] **Fixture-parity suites green with EXACT float equality:** pyrand, mat3, posterior, discount, update, ceiling, sampling, pid, state_dict, rails, floors, vegas, market, cycle — every expected value compared as a `py_repr` string, zero epsilon comparisons.
- [ ] Conformance scenarios 08–13 replay byte/content-identically; 14–17 relabeled to Phase 5; `all_scenarios_replayed_or_deferred` green (T12, after Phase 3's flip).
- [ ] Dry-run journal produced on lnnode (read-only DB, shadow names) and `diff_fee_decisions.py` reports ZERO deterministic-field mismatches over the compressed live window (hours-level per the deadline addendum); sampled fees all within [floor, ceiling] and delta caps. **Fee cutover only after this — cutover wiring itself is out of this plan's scope.**
- [ ] Governed path pinned: fail-closed `internal_error ({e})`, zero-cost SET_FEE intent shape, no `budget_reserved` events for reversible fee changes.
- [ ] `cargo fmt --check`, `cargo clippy --workspace -- -D warnings`, `cargo test --workspace` green in CI; generator committed on branch `port` (never `main`).

## Self-Review Notes

- **Spec coverage:** DTS (T1/T2/T3/T7), PID (T3), Vegas (T6), market intel (T8), chain/rebalance floors (T6), congestion episodes + zero-flow ratchet + drain bias (T5/T6/T10 stage 4/5), admission valve + goldens (T4), v2_state_json lossless round-trip (T3/T9), ADR-001 rail order + per-rail fixtures (T5/T6/T10, conformance 08–11), dry-run journal + diff instrument (T10/T11), un-defer flip (T12). Failed-forward nudge math ported (T7); its hook wiring is explicitly deferred to `crates/revops` with the cutover work.
- **Known risks accepted:** libm ULP divergence (canaried by the pyrand/gauss and posterior suites — if any fixture fails only on exotic values, escalate to the controller rather than loosening to epsilon); `PyRandom` stream desync via branch-order mistakes (mitigated by sequence fixtures that pin multi-call patterns and the Vegas short-circuit pin); the root-`Cargo.toml` one-line merge point with Phase 3.
- **Type consistency check:** `FeeProfileSettings` is defined once (T5 `profiles.rs`) and consumed by T5/T10 signatures; `Observation`/`GaussianThompsonState` defined in `thompson/{recompute,mod}.rs` per the T2/T3 coordination note; `pyjson::OValue` is the single ordered-JSON type used by T3 serde, T9 envelope, and T10 journal.
