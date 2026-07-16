# Phase 2: Governed Economic Core (revops-econ) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Tasks are sized for one implementer subagent each and grouped into parallel waves (see Wave Table) for isolated worktrees per superpowers:using-git-worktrees.

**Goal:** A new `crates/revops-econ` library crate that ports the Python governed-economics layer (econ_types → reason_codes → cycle_context → econ_snapshot → econ_intents → econ_ev → econ_ledger → econ_arbiter → governor_facade → econ_reconcile → econ_cycle) byte-parity against the existing 40-scenario conformance corpus. Exit gate: a Rust fixture-runner consumes the SAME `tests/conformance/scenarios/*/case.json` files (vendored byte-for-byte) and replays all 18 econ-core scenarios to byte-identical results; the remaining 22 scenarios are schema-gated with an explicit, test-pinned skip list (owned by Phases 3–6).

**Deadline context (2026-07-19 addendum):** This plan is one of the parallel workstreams. All waves are executable in ~1 day: Wave 0 is ~1 hour, Waves 1–3 are 4-way/2-way parallel, Wave 4 is the gate. The conformance byte-parity gate is on the NOT-compressed floor list — it must actually pass, not be waived.

**Python source of truth:** `~/bin/cl_revenue_ops-port` @ branch `port` (v2.18.1), `modules/econ_*.py`, `modules/reason_codes.py`, `modules/cycle_context.py`, `modules/governor_facade.py`. Corpus: `tests/conformance/scenarios/` (40 dirs, each a `case.json`; scenario 40 adds `expected-projections.json` + `expected-ledger-events.json`). Generator (reference semantics, incl. the pinned `NOW = 1_752_400_000` and wire shapes `_arb_wire`/`_decision_wire`): `tools/conformance/generate_scenarios.py`. Python validator (schema-only, no plugin imports): `tools/conformance/validate_fixtures.py`.

## Global Constraints

- Every file in `crates/revops-econ`: crate root has `#![forbid(unsafe_code)]`; clippy warnings deny in CI.
- **Workspace dep set only.** Task 1 adds to the ROOT `Cargo.toml` `[workspace.dependencies]`: `sha2 = "0.10"`, `hex = "0.4"`, `rusqlite = "0.32"` (features = [] — system sqlite, NEVER "bundled"), `tempfile = "3"` (dev). No other new crates (no `regex` — hand-roll the three ID validators; no `jsonschema` — the Rust schema gate is typed-struct parsing; no `rand` — `CycleContext.rng()` is deliberately NOT ported, nothing pinned consumes it).
- **Canonical JSON only via `revops_core::canonical::canonical_json(&Value) -> Result<String, CanonicalError>`** — it is Result-returning and rejects ALL float-typed numbers fail-closed. Idempotency keys, snapshot hashing, and conformance comparisons go through it exclusively.
- **No f64 anywhere in money paths.** Python `econ_types` is checked integer math; port it as checked `i64` (u63 range enforced). Floats exist at exactly THREE quarantined ingress points, each with a distinct rounding rule that must not be conflated:
  1. `Micro::from_float_clamped` — clamp to [0,1] then **half-UP via truncation**: `(x * 1e6 + 0.5) as i64` (Python `int(x*1e6 + 0.5)`).
  2. `econ_ev::confidence_micro` — clamp then **banker's rounding**: `f64::round_ties_even` (Python `round()`).
  3. `econ_ev::benefit_msat_from_sats` — `(v * 1000.0).round_ties_even()` (Python `int(round(v*1000))`).
- Error semantics: every bound violation/unit-mixing returns `Err(EconError)` (mirror of `EconArithmeticError`) — authorization arithmetic **fails closed, never wraps, never coerces to zero**. Shadow/observe paths fail OPEN; authorization paths fail CLOSED (asymmetry is the point — don't unify).
- Rounding directions (already in revops-core, reuse the rules): msat→sat CEIL for fees/budgets/costs/revenue, FLOOR for capacity/balances, toward-zero for signed deltas (Rust `/` on i64 already truncates toward zero).
- Wire strings are frozen: reason codes, intent types, enums (UPPER_SNAKE), arbitration detail strings ("duplicate idempotency key", "target {t} has a close intent", …), `"int-"+key[:16]`, `"auth-"+key[:16]`. Wording changes are conformance failures.
- Strict TDD: each task writes its failing tests first (unit tests transcribing the Python behaviors + the corpus values named in the task), then implements.
- Only Task 1 touches shared files (root `Cargo.toml`, `crates/revops-econ/Cargo.toml`, `lib.rs`). Task 1 pre-declares **all** modules in `lib.rs` with stub files so Waves 1–4 never edit shared files → parallel-worktree safe.

## Wave Table (parallelism)

| Wave | Tasks | Parallel-safe? |
|---|---|---|
| 0 | T1 scaffold + types + reason codes + context | sequential gate (everything depends on it) |
| 1 | T2 snapshot · T3 intents+ev · T4 ledger | 3-way parallel (disjoint files) |
| 2 | T5 arbiter+registry (needs T3) · T6 reconcile (needs T4) | 2-way parallel |
| 3 | T7 governor (needs T3,T4,T5) · T8 cycle (needs T3,T5) | 2-way parallel |
| 4 | T9 conformance corpus vendoring + Rust fixture-runner (needs all) | gate |
| 4* | T10 shadow hub (needs T7) — non-gating stretch | parallel with T9 (disjoint files) |

Deferred out of this plan (explicitly, so it's not a silent omission): `risk_profiles.py` (static tables, no corpus coverage — schedule with the config workstream/Phase 6 RPC surface), `run_shadow_cycle`'s live collector + the `revenue-econ-*` RPC registrations in `crates/revops` (Phase 2b wiring, after the diff harness can exercise them).

---

### Task 1 (Wave 0): Crate scaffold + checked types + reason codes + cycle context

**Mirrors:** `modules/econ_types.py` (whole file), `modules/reason_codes.py` (whole file), `modules/cycle_context.py` (`CycleContext`, `derive_seed`; `rng()` intentionally omitted).

**Files:**
- Modify: `Cargo.toml` (root — add `crates/revops-econ` to members; add workspace deps `sha2`, `hex`, `rusqlite`, `tempfile`)
- Create: `crates/revops-econ/Cargo.toml`
- Create: `crates/revops-econ/src/lib.rs` (declares ALL modules for the whole phase: `types`, `reason`, `context`, `snapshot`, `intents`, `ev`, `arbiter`, `governor`, `ledger`, `reconcile`, `cycle`, `shadow` — stub files with doc comments only, so later tasks never touch lib.rs)
- Create: `crates/revops-econ/src/types.rs`, `src/reason.rs`, `src/context.rs` (+ stub `snapshot.rs`, `intents.rs`, `ev.rs`, `arbiter.rs`, `governor.rs`, `ledger.rs`, `reconcile.rs`, `cycle.rs`, `shadow.rs`)

```toml
# crates/revops-econ/Cargo.toml
[package]
name = "revops-econ"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
revops-core = { path = "../revops-core" }
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
sha2.workspace = true
hex.workspace = true
rusqlite.workspace = true

[dev-dependencies]
tempfile.workspace = true
```

- [ ] **Step 1: failing tests** — `types.rs` unit tests transcribing: `Msat(2**63)` and `Msat(-1)` rejected (corpus s32 pins classifier "EconArithmeticError"); `Msat.sub` underflow; `to_sats_floor/ceil` over `[999,1000,1001,1999]` == `[[0,1,1,1],[1,1,2,2]]` (corpus s33 exact values); `SignedMsat::to_sats_toward_zero(-1999) == -1`; `Ppm::fee_ceil/fee_floor`; `Micro::from_float_clamped(0.5000005)==500001` (half-up), NaN/inf rejected; ID validators accept `"111x222x0"` / `"02"+"b"*64` and reject uppercase hex, `"04.."` prefixes, empty intent ids. `context.rs`: `derive_seed` golden values computed from Python (generate 5 pinned pairs with `python3 -c` from the port worktree and hardcode them, e.g. seed 0 + `"econ-cycle"`).
- [ ] **Step 2: implement.** Core contract sketch (complete, not a placeholder):

```rust
// types.rs
pub const U63_MAX: i64 = i64::MAX;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{msg}")]
pub struct EconError { pub msg: String }   // mirror of EconArithmeticError: hard stop, never zero
pub type EconResult<T> = Result<T, EconError>;

fn check_int(value: i128, low: i128, high: i128, kind: &str) -> EconResult<i64> {
    if value < low || value > high {
        return Err(EconError { msg: format!("{kind} out of range [{low}, {high}]: {value}") });
    }
    Ok(value as i64)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Msat(i64);
impl Msat {
    pub fn new(value: i64) -> EconResult<Self> { Self::from_checked(value as i128) }
    /// i128 entry so conformance s32 can probe 2**63 (unrepresentable in i64).
    pub fn from_checked(value: i128) -> EconResult<Self> {
        Ok(Msat(check_int(value, 0, U63_MAX as i128, "Msat")?))
    }
    pub const fn value(self) -> i64 { self.0 }
    pub fn add(self, other: Msat) -> EconResult<Msat> {
        Msat::from_checked(self.0 as i128 + other.0 as i128)
    }
    pub fn sub(self, other: Msat) -> EconResult<Msat> {
        if self.0 < other.0 {
            return Err(EconError { msg: format!("Msat.sub underflow: {} - {}", self.0, other.0) });
        }
        Ok(Msat(self.0 - other.0))
    }
    pub fn diff(self, other: Msat) -> SignedMsat { SignedMsat(self.0 - other.0) } // both u63: fits i64
    /// Fees, budgets, costs, revenue reporting: round UP.
    pub fn to_sats_ceil(self) -> Sat { Sat(((self.0 as u64).div_ceil(1000)) as i64) }
    /// Capacity and balances: round DOWN.
    pub fn to_sats_floor(self) -> Sat { Sat(self.0 / 1000) }
    pub fn from_sats(sats: i64) -> EconResult<Msat> {
        check_int(sats as i128, 0, U63_MAX as i128, "Msat.from_sats")?;
        Msat::from_checked(sats as i128 * 1000)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SignedMsat(pub i64); // range is exactly i64 — construction infallible from i64
impl SignedMsat {
    /// Signed deltas: round toward zero (Rust integer division already truncates).
    pub fn to_sats_toward_zero(self) -> i64 { self.0 / 1000 }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Micro(i64); // [0, 1_000_000]
impl Micro {
    pub fn new(value: i64) -> EconResult<Self> {
        Ok(Micro(check_int(value as i128, 0, 1_000_000, "Micro")?))
    }
    /// THE only float ingress in econ_types: clamp then round HALF-UP by
    /// truncation — Python `int(clamped * 1e6 + 0.5)`. (econ_ev uses
    /// banker's rounding instead — do not unify.)
    pub fn from_float_clamped(f: f64) -> EconResult<Self> {
        if !f.is_finite() {
            return Err(EconError { msg: format!("Micro.from_float_clamped: {f:?}") });
        }
        Ok(Micro((f.clamp(0.0, 1.0) * 1_000_000.0 + 0.5) as i64))
    }
    pub const fn value(self) -> i64 { self.0 }
}
```

Plus `Sat`, `Ppm` (`fee_ceil` = `-(-(amount*ppm)//1_000_000)` → `(amount as i128 * ppm as i128).div_ceil(1_000_000)` checked back to u63; `fee_floor` analogous), `UnixTime` (`plus_seconds` checked), and hand-rolled validated string newtypes `ChannelId` (`^[0-9]+x[0-9]+x[0-9]+$`), `PeerId` (66 chars, `02|03` prefix, lowercase hex), `IntentId` (`^[a-z0-9-]{1,64}$`).

```rust
// context.rs — derive_seed is a stable cross-language contract (pure sha256)
use sha2::{Digest, Sha256};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CycleContext {
    pub cycle_id: String, pub cycle_time: UnixTime, pub seed: i64, pub snapshot_id: String,
}
impl CycleContext {
    pub fn new(cycle_id: String, cycle_time: UnixTime, seed: i64, snapshot_id: String) -> EconResult<Self> {
        if cycle_id.is_empty() || snapshot_id.is_empty() || seed < 0 {
            return Err(EconError { msg: format!("CycleContext invalid: {cycle_id:?}/{snapshot_id:?}/{seed}") });
        }
        Ok(Self { cycle_id, cycle_time, seed, snapshot_id })
    }
    /// first 8 bytes big-endian of sha256("{seed}:{component}") masked to u63.
    pub fn derive_seed(&self, component: &str) -> EconResult<i64> {
        if component.is_empty() {
            return Err(EconError { msg: format!("derive_seed component invalid: {component:?}") });
        }
        let digest = Sha256::digest(format!("{}:{}", self.seed, component).as_bytes());
        let mut b = [0u8; 8];
        b.copy_from_slice(&digest[..8]);
        Ok((u64::from_be_bytes(b) & 0x7fff_ffff_ffff_ffff) as i64)
    }
}
```

`reason.rs`: `pub enum Code` with the 18 wire-frozen variants exactly as in `reason_codes.py` lines 26–46 (`BUDGET_EXHAUSTED`, `AUTHORITY_LEVEL_BLOCKED`, `PAUSED`, `INTENT_STALE`, `INTENT_SUPERSEDED`, `CHANNEL_PROTECTED`, `CONTRACT_OBLIGATION`, `EV_BELOW_HOLD_MARGIN`, `INSUFFICIENT_CONFIDENCE`, `FEE_RAIL_CLAMPED`, `COOLDOWN_ACTIVE`, `CONFLICT_CLOSE_REBALANCE`, `CONFLICT_DUPLICATE_OPEN`, `CONFLICT_REBALANCE_SWAP`, `EXTERNAL_CIRCUIT_BREAKER`, `EXTERNAL_OUTCOME_UNKNOWN`, `ARITHMETIC_OVERFLOW`, `SCHEMA_INVALID`) + `as_str()`, `layer()`, `kind()`, `is_valid_code(&str) -> bool`; a test pins the count == 18 and every (layer, kind) tuple.

- [ ] **Step 3:** `cargo test -p revops-econ`, `cargo clippy --workspace -- -D warnings` green. Commit.

---

### Task 2 (Wave 1, parallel): Canonical economic snapshot

**Mirrors:** `modules/econ_snapshot.py` — `ROLES`/`LIFECYCLES` frozensets, `Protection`, `ChannelSnapshot`, `BudgetState`, `NodeState`, `EconomicSnapshot` (J3 channel sort at construction), `_channel_wire`/`to_wire`, `build_channel_snapshot`.

**Files:** Create `crates/revops-econ/src/snapshot.rs` (replacing stub), `crates/revops-econ/tests/snapshot.rs`.

**Interfaces:**
```rust
pub const SCHEMA_NAME: &str = "economic_snapshot";
pub const SCHEMA_VERSION: i64 = 0;
pub use revops_core::canonical::{canonical_json, CanonicalError}; // the ONE canonical serializer

pub struct Protection { pub reason: String, pub owner: String, pub expires_at: Option<UnixTime> }
pub struct ChannelSnapshot { /* all 20 fields, exact names/types from econ_snapshot.py lines 66–86 */ }
pub struct NodeState { /* 6 fields + pending_operations/external_obligations: Vec<serde_json::Value> */ }
pub struct EconomicSnapshot { /* snapshot_id, observed_at, evidence_window_seconds, node, channels */ }
impl EconomicSnapshot {
    /// Sorts channels by channel_id at construction (J3) and validates.
    pub fn new(...) -> EconResult<Self>;
    pub fn to_wire(&self) -> serde_json::Value;   // field order irrelevant — canonical_json sorts
}
pub struct ProfEvidence {  // duck-typed `prof` made explicit
    pub fees_earned_msat: i64, pub sourced_fee_contribution_msat: i64,
    pub rebalance_cost_sats: i64, pub open_cost_sats: i64,
    pub net_profit_sats: i64, pub volume_routed_msat: i64,
    pub forward_count: i64, pub sourced_forward_count_30d: i64,
}
pub fn build_channel_snapshot(channel: &serde_json::Value, prof: Option<&ProfEvidence>,
    flow_confidence: Option<f64>, role: &str, lifecycle: &str,
    protections: Vec<Protection>) -> EconResult<ChannelSnapshot>;
```

- [ ] Failing tests first: role/lifecycle enum rejection; negative forward_count rejection; channel auto-sort by channel_id; `prof=None` ⇒ all-zero economics + `Micro(0)` confidence (invariant 7 — missing evidence never invents values); `to_wire` key set matches `economic_snapshot.v0.schema.json`; `canonical_json(to_wire)` of a two-channel snapshot equals a golden string generated once from Python (`python3 - <<'EOF'` in the port worktree, hardcoded in the test).
- [ ] Implement; `net_value_msat = prof.net_profit_sats * 1000` checked; costs use `Msat::from_sats`. Commit.

---

### Task 3 (Wave 1, parallel): Intent envelopes + idempotency keys + EV contract

**Mirrors:** `modules/econ_intents.py` (whole file) and `modules/econ_ev.py` (whole file).

**Files:** Create `crates/revops-econ/src/intents.rs`, `src/ev.rs`, `crates/revops-econ/tests/intents.rs`.

**Interfaces + the tricky contract in full:**
```rust
// intents.rs
pub const SCHEMA_NAME: &str = "intent";
pub const SCHEMA_VERSION: i64 = 0;
pub const INTENT_TYPES: [&str; 9] = ["SET_FEE","SET_HTLC_MAX","REBALANCE","OPEN_CHANNEL",
    "CLOSE_CHANNEL","SWAP_IN","SWAP_OUT","JOIN_LIQUIDITY_SWAP","MAINTAIN_ONCHAIN_RESERVE"];

#[derive(Debug, Clone, PartialEq)]
pub struct Explanation { pub kind: String, pub components: Vec<(String, serde_json::Value)> }
impl Explanation {
    /// Python str() semantics per value: String → raw (unquoted), integer →
    /// digits, bool → "True"/"False", null → "None", float → NOT SUPPORTED
    /// here in Phase 2 (see cycle.rs pyfloat note). Pinned by unit tests.
    pub fn render(&self) -> String; // "kind: k=v, k=v"
}

/// Deterministic key (J3): canonical JSON of EXACTLY these five fields,
/// sha256 hex. amount None → JSON null. Byte-exact with Python:
/// sha256(canonical_json({"amount_msat":…,"budget_bucket":…,"intent_type":…,
/// "snapshot_id":…,"target":…})) — key order irrelevant, canonical sorts.
pub fn compute_idempotency_key(intent_type: &str, target: &str, amount_msat: Option<i64>,
                               snapshot_id: &str, budget_bucket: &str) -> String {
    use sha2::{Digest, Sha256};
    let subset = serde_json::json!({
        "intent_type": intent_type,
        "target": target,
        "amount_msat": amount_msat,
        "snapshot_id": snapshot_id,
        "budget_bucket": budget_bucket,
    });
    let canon = revops_core::canonical::canonical_json(&subset)
        .expect("five-field subset is integer/string/null only");
    hex::encode(Sha256::digest(canon.as_bytes()))
}

pub struct IntentEnvelope { /* all 20 fields from econ_intents.py lines 84–105, exact names */ }
pub struct IntentFields { /* everything except intent_id/idempotency_key — the make_intent input */ }
/// intent_id = "int-" + key[:16]; validation mirrors __post_init__ exactly
/// (type in INTENT_TYPES, non-empty snapshot_id/target/bucket/origin,
/// expires_at > created_at, 0<=priority<=100, all reason codes in CATALOG).
pub fn make_intent(fields: IntentFields) -> EconResult<IntentEnvelope>;
/// Inclusive boundary: now >= expires_at counts as expired (corpus s34 pins
/// probes NOW+599/600/601 -> [false, true, true]).
pub fn is_expired(env: &IntentEnvelope, now: UnixTime) -> bool;
pub fn to_wire(env: &IntentEnvelope) -> serde_json::Value;   // explanation.components as [[name,value],…]
pub fn from_wire(d: &serde_json::Value) -> EconResult<IntentEnvelope>; // strict schema_name/version check

// ev.rs
pub fn expected_value_msat(revenue_msat: i64, execution_cost_msat: i64,
    capital_cost_msat: i64, risk_premium_msat: i64) -> EconResult<SignedMsat>; // checked i128 subtraction
/// None/NaN/inf → 0 (conservative). Finite → banker's: (v*1000).round_ties_even().
pub fn benefit_msat_from_sats(value_sats: Option<f64>) -> EconResult<SignedMsat>;
/// None/NaN/inf → 0; else clamp [0,1] then banker's round to micro.
pub fn confidence_micro(fraction: Option<f64>) -> Micro;
```

- [ ] Failing tests first, including corpus-derived golden values: reconstruct generator `_env()` defaults (`REBALANCE`, target `111x222x0`, amount 400_000 sats → 400_000_000 msat, snapshot `snap-1`, bucket `rebalance`, created `1_752_400_000`, expires +600) and assert `intent_id`/`idempotency_key` equal the exact strings in `tests/conformance/scenarios/31-duplicate-idempotency-key/case.json` and `37-clock-seed-determinism/case.json` (read those two values from Python corpus files and hardcode). `from_wire(to_wire(env)) == env`. Banker's-vs-half-up test: `confidence_micro(0.5000005)` vs `Micro::from_float_clamped(0.5000005)` differ exactly as Python's do.
- [ ] Implement; commit.

---

### Task 4 (Wave 1, parallel): Append-only econ ledger + replay

**Mirrors:** `modules/econ_ledger.py` — `EVENT_TYPES` (13), `_TERMINAL_EVENTS`, `EconLedger.append/events/count_events/replay`, `LedgerState`.

**Files:** Create `crates/revops-econ/src/ledger.rs`, `crates/revops-econ/tests/ledger.rs`.

**Interfaces:**
```rust
pub const EVENT_TYPES: [&str; 13] = [ /* exact list incl. "snapshot_created" */ ];
pub struct EconLedger { path: std::path::PathBuf }
pub struct LedgerEvent { pub event_id: i64, pub event_type: String, pub intent_id: String,
    pub idempotency_key: String, pub cycle_id: String, pub at: i64,
    pub amounts: std::collections::BTreeMap<String, i64>, pub details: serde_json::Value }
#[derive(Default, Debug, PartialEq)]
pub struct LedgerState { pub reserved_msat: BTreeMap<String, i64>, pub spent_msat: BTreeMap<String, i64>,
    pub total_spent_msat: i64, pub terminal: BTreeMap<String, String>, pub anomalies: Vec<String> }
impl EconLedger {
    /// Opens/creates econ_ledger_events with the EXACT Python DDL. Like
    /// Python, a fresh connection per operation (thread-affinity incident
    /// 2026-07-12); rusqlite busy_timeout = 5000ms on every open.
    pub fn open(path: impl Into<PathBuf>) -> EconResult<Self>;
    pub fn append(&self, event_type: &str, intent_id: &str, idempotency_key: &str,
                  cycle_id: &str, at: i64, amounts: &BTreeMap<String, i64>,
                  details: &serde_json::Value) -> EconResult<i64>;
    pub fn events(&self, since_id: i64) -> EconResult<Vec<LedgerEvent>>;
    pub fn count_events(&self, event_type: Option<&str>) -> EconResult<i64>;
    pub fn replay(&self) -> EconResult<LedgerState>;
}
/// Python json.dumps(x, sort_keys=True) with DEFAULT separators (", ", ": ")
/// — NOT the canonical compact form. Rust-written amounts_json/details_json
/// must stay byte-compatible with Python-written rows for dual-read of one
/// econ_ledger.db during the shadow window.
pub(crate) fn python_dumps_default(v: &serde_json::Value) -> EconResult<String>;
```

Replay rules to port verbatim (the budget-truth contract): `budget_reserved` SETS (idempotent re-announce); `cost_recorded` adds spend + decrements reservation floored at 0, and if `reserved.get(key,0) <= 0 && !spent.contains(key)` push anomaly string exactly `"cost_recorded without reservation: {key}"` (spend is never free); `reservation_released` zeroes; `reconciliation_completed` SETS `reserved_msat` absolutely if present, adds `cost_msat` if present, terminal only when `details.terminal` truthy; the five terminal events use first-wins (`entry().or_insert`); `reserved_msat` output drops zero entries; validation: unknown event type / empty ids / negative `at` / bool-or-out-of-range amounts → `EconError`.

- [ ] Failing tests first, taken from corpus scenarios 25, 26, 40: replay of `40-sanitized-production-decisions/expected-ledger-events.json`'s three events must produce exactly `expected-projections.json` (`reserved_msat={}`, `spent_msat={}`, `total_spent_msat=0`, `terminal={}`, `anomalies=[]`); s25 orphan cost anomaly; s26 unknown-outcome terminal with reservation preserved (`{"k-26": 5000}`).
- [ ] Implement with tempfile-backed DBs in tests; commit.

---

### Task 5 (Wave 2, parallel): Batch arbiter + live ActiveIntentRegistry

**Mirrors:** `modules/econ_arbiter.py` — `PRECEDENCE`, `DEFAULT_INTENT_PRECEDENCE`, `_sort_key`, `arbitrate` (rules 1–6), `ActiveIntentRegistry` (live semantics DIFFER from batch — port both, don't unify).

**Files:** Create `crates/revops-econ/src/arbiter.rs`, `crates/revops-econ/tests/arbiter.rs`.

**Interfaces:**
```rust
pub fn precedence_class(env: &IntentEnvelope) -> i64; // contractual_obligation 0 .. growth 6
pub struct ArbitrationResult {
    pub ordered: Vec<IntentEnvelope>,
    pub rejected: Vec<(IntentEnvelope, String /*reason_code*/, String /*detail*/)>,
    pub superseded: BTreeMap<String, String>, // rejected intent_id -> superseding id
}
/// Pure + deterministic: same intents + same now => byte-identical result
/// regardless of input order. Sort key: (precedence, -priority,
/// -expected_benefit, -confidence, capital_committed, target, intent_id).
pub fn arbitrate(intents: &[IntentEnvelope], now: i64, extended_rules: bool) -> ArbitrationResult;

pub struct ActiveIntentRegistry { inner: std::sync::Mutex<RegistryInner>,
    extended_rules_provider: Option<Box<dyn Fn() -> bool + Send + Sync>> }
impl ActiveIntentRegistry {
    pub fn new(extended_rules_provider: Option<Box<dyn Fn() -> bool + Send + Sync>>) -> Self;
    /// Some(reason_code) blocking, or None (then registered). Prunes
    /// now >= expires_at first. LIVE semantics: duplicate key →
    /// INTENT_SUPERSEDED; REBALANCE vs active CLOSE_CHANNEL same target →
    /// CONFLICT_CLOSE_REBALANCE; extended: OPEN_CHANNEL dup (first-registered
    /// wins) → CONFLICT_DUPLICATE_OPEN; REBALANCE/SWAP_OUT either direction
    /// (second arrival blocked) → CONFLICT_REBALANCE_SWAP. Provider error/
    /// None → legacy rules only (fail to current behavior, never stricter).
    pub fn check_and_register(&self, env: &IntentEnvelope, now: i64) -> Option<&'static str>;
    pub fn release(&self, idempotency_key: &str);
    pub fn active_count(&self, now: i64) -> usize;
}
```

Detail strings frozen: `"expired before arbitration"`, `"duplicate idempotency key"`, `"conflicting {intent_type} on {target}"`, `"target {t} has a close intent"`, `"target {t} already has an open intent"`, `"target {t} has a structural swap intent"`. Rule order frozen: stale → dup keys (best-sorted wins) → SET_FEE/SET_HTLC_MAX per (type,target) → close-vs-rebalance → [extended] dup-open → rebalance-vs-SWAP_OUT (swap outranks).

- [ ] Failing tests first: replay corpus s18, s20, s21, s31, s35, s38 inputs (build envelopes via `make_intent` with the generator's `_env` parameters) and assert ordered ids + (reason_code, detail) tuples equal the corpus `expected` values verbatim; order-insensitivity test (shuffle inputs → identical result); registry live-vs-batch divergence tests (close registered after rebalance still blocks the rebalance? No — live blocks the SECOND arrival: pin both directions).
- [ ] Implement (stable `sort_by` with owned key tuples); commit.

---

### Task 6 (Wave 2, parallel): Ledger↔DB reconciliation

**Mirrors:** `modules/econ_reconcile.py` — `Divergence`, `ReconciliationReport`, `_started_without_terminal`, `reconcile`, `fee_intent_completeness`, `apply`.

**Files:** Create `crates/revops-econ/src/reconcile.rs`, `crates/revops-econ/tests/reconcile.rs`.

**Interfaces:**
```rust
pub struct DbReservationState { pub status: String, pub reserved_sats: i64 }
pub struct Divergence { pub kind: String, pub key: String, pub ledger_reserved_msat: i64,
    pub db_status: Option<String>, pub db_reserved_sats: Option<i64>,
    pub resolution: Option<serde_json::Value>, pub details: serde_json::Value }
pub struct ReconciliationReport { pub checked: usize, pub matched: usize, pub divergences: Vec<Divergence> }
/// The LEDGER reconciles TO the DB. DB terminal statuses {"spent","released"}.
/// In-flight keys (execution_started, no terminal) are EXCLUDED from
/// resolvable classification; stale ones (> stale_after_seconds, default
/// 3600) quarantine as kind="unknown_outcome" with resolution=None and
/// details.reason_code=EXTERNAL_OUTCOME_UNKNOWN — never auto-resolved.
/// Keys iterated sorted (determinism). Kinds: db_missing (→ reserved_msat 0),
/// ledger_stale_reservation (→ 0), ledger_missing_reservation (→ db_sats*1000),
/// amount_mismatch (→ db_sats*1000).
pub fn reconcile(ledger: &EconLedger, db_states: &BTreeMap<String, DbReservationState>,
                 now: i64, stale_after_seconds: i64) -> EconResult<ReconciliationReport>;
/// Timestamp clustering within tolerance_seconds=120 (live 2026-07-12 pin);
/// cycle ids fee-cycle-<ts> / fee-broadcast-<ts>; window vs first intent.
pub fn fee_intent_completeness(ledger: &EconLedger, fee_changes: &[serde_json::Value],
                               now: i64, window_seconds: i64, tolerance_seconds: i64)
                               -> EconResult<serde_json::Value>;
/// One reconciliation_completed per RESOLVABLE divergence (details += kind);
/// intent_id = key[:16] (or key if shorter); cycle_id "reconcile".
pub fn apply(ledger: &EconLedger, report: &ReconciliationReport, now: i64) -> EconResult<usize>;
```

- [ ] Failing tests first: corpus s27 semantics (execution_started at NOW−7200, empty db_states, stale=3600 → one `unknown_outcome` quarantined, NOT db_missing); each of the four resolvable kinds; `reconciliation_completed` events terminal per replay; fee-completeness clustering test transcribed from `tests/test_econ_reconcile.py` (3 rows at :41 + 5 at :42 = one 8-change cycle).
- [ ] Implement; commit.

---

### Task 7 (Wave 3, parallel): Governor facade + authority ladder

**Mirrors:** `modules/governor_facade.py` — `AUTHORITY_LEVELS`, `authority_allows`, `AuthorizationToken`, `GovernorDecision`, `GovernorFacade.authorize/release`.

**Files:** Create `crates/revops-econ/src/governor.rs`, `crates/revops-econ/tests/governor.rs`.

**Interfaces + tricky contract in full:**
```rust
pub fn authority_allows(configured_level: Option<&str>, required_level: &str) -> bool {
    // trim+lowercase; unknown configured -> 0 (observe: a typo never grants
    // authority); unknown required -> 3 (capital). configured >= required.
}

pub struct AuthorizationToken { pub token_id: String, pub intent_id: String,
    pub reservation_id: String, pub reserved_msat: i64, pub budget_bucket: String,
    pub issued_at: i64, pub arbitration_key: String }
pub struct GovernorDecision { pub authorized: bool,
    pub token: Option<AuthorizationToken>, pub reason_code: String }

pub struct GovernorFacade<'a> {
    /// (reservation_id, amount_sats, category) -> granted. Errors fail CLOSED.
    pub reserve_spend: &'a dyn Fn(&str, i64, &str) -> EconResult<bool>,
    pub release_spend: &'a dyn Fn(&str) -> EconResult<bool>,
    pub is_paused: &'a dyn Fn() -> bool,
    pub ledger: Option<&'a EconLedger>,
    pub registry: Option<&'a ActiveIntentRegistry>,
    /// Err => fail closed (mirrors Python catching Exception -> False).
    pub authority_check: Option<&'a dyn Fn() -> EconResult<bool>>,
}
impl GovernorFacade<'_> {
    /// Decision order FROZEN: paused -> PAUSED; authority (fail closed on
    /// error) -> AUTHORITY_LEVEL_BLOCKED; expired -> INTENT_STALE; registry
    /// conflict -> conflict code (ledger intent_rejected best-effort,
    /// swallowed); reserve max_cost msat->sat CEIL under the CALLER's
    /// reservation_id (2026-07-13 phantom-reservation fix) ->
    /// BUDGET_EXHAUSTED on refusal. reserve_sats == 0 (reversible fee/HTLC
    /// change): authorize WITHOUT reservation and WITHOUT budget_reserved
    /// event. Ledger keying: intent_authorized under env.idempotency_key;
    /// budget_reserved under effective_reservation_id. token_id =
    /// "auth-" + key[:16]; reserved_msat = reserve_sats * 1000.
    pub fn authorize(&self, env: &IntentEnvelope, now: i64,
                     reservation_id: Option<&str>) -> EconResult<GovernorDecision>;
    /// Registry release (arbitration_key else reservation_id, best-effort),
    /// then release_spend, then reservation_released event (cycle_id
    /// "release").
    pub fn release(&self, token: &AuthorizationToken, now: i64) -> EconResult<bool>;
}
```

- [ ] Failing tests first, replaying corpus authorization cases: s22 (reserve refused → `{authorized:false, reason_code:"BUDGET_EXHAUSTED"}`), s30 (stale → `INTENT_STALE`), s29 (authority observe vs required capital → `AUTHORITY_LEVEL_BLOCKED`; ungated path → authorized) — expected dicts byte-equal via canonical JSON. Plus: decision-order pins (paused beats everything; authority beats staleness), zero-cost no-reservation-event pin (count `budget_reserved` events == 0), authority_check `Err` → blocked, ledger keying pin (intent_authorized under envelope key, budget_reserved under caller's `r-1`).
- [ ] Implement; commit.

---

### Task 8 (Wave 3, parallel): Deterministic cycle core

**Mirrors:** `modules/econ_cycle.py` — `CycleResult` + `to_wire`/`canonical`, `rebalance_intent_pairs`, `rebalance_intents_from_pairs`, `_rebalance_intent`, `plan_cycle`. (`run_shadow_cycle` live collector deferred to Phase 2b wiring.)

**Files:** Create `crates/revops-econ/src/cycle.rs`, `src/pyfloat.rs` (add `pub mod pyfloat;` was pre-declared in Task 1's lib.rs — if not, declare it in cycle.rs as a child module to avoid touching lib.rs), `crates/revops-econ/tests/cycle.rs`.

**Interfaces:**
```rust
pub struct RebalancePair { pub source_channel_id: String, pub dest_channel_id: String,
    pub amount_sats: i64, pub pair_budget_sats: i64, pub score: f64,
    pub score_decomposition: Option<serde_json::Value> }
pub struct CycleResult { pub context: CycleContext, pub channel_count: i64,
    pub intents: Vec<IntentEnvelope>, pub arbitration: ArbitrationResult }
impl CycleResult {
    pub fn to_wire(&self) -> serde_json::Value;   // econ_cycle_result v0 shape, exact keys
    pub fn canonical(&self) -> EconResult<String>; // see pyfloat note below
}
/// Pairs sorted by (dest, source); envelope: 600s expiry, priority 50,
/// bucket "rebalance", origin "econ_cycle_shadow", reversible=false,
/// amount/capital = amount_sats*1000, max_cost = pair_budget_sats*1000;
/// explanation ("cycle_rebalance", source/dest/amount_sats/score).
pub fn rebalance_intent_pairs(pairs: &[RebalancePair], ctx: &CycleContext, ev_enabled: bool)
    -> EconResult<Vec<(IntentEnvelope, usize /*pair index*/)>>;
pub fn plan_cycle(pairs: &[RebalancePair], ctx: &CycleContext, channel_count: i64)
    -> EconResult<CycleResult>;
```

**Float-in-explanation hazard (the one place floats reach the wire):** Python puts `round(float(score), 6)` into the explanation components, so `CycleResult.to_wire()` legitimately contains a float — which `revops_core::canonical_json` rejects fail-closed (correctly: it also feeds idempotency keys, which never include explanations). Resolution: `pyfloat.rs` implements `pub fn py_repr(f: f64) -> String` mirroring CPython `repr(float)` (shortest round-trip; exponent format for `abs >= 1e16` or `< 1e-4`, rendered as `1e-05`/`1e+16` style; integral floats as `5.0`), pinned by a fixture file generated from Python (`tools/port/gen_pyfloat_fixtures.py` in the port worktree — commit there, copy JSON here). `CycleResult::canonical()` uses a local canonical writer that delegates to `revops_core` semantics but formats float-typed numbers via `py_repr` — used ONLY for shadow-cycle wire publishing, NEVER for idempotency keys. The conformance corpus contains no float wire values, so the gate does not depend on `py_repr`; the live diff harness verifies it during shadow.

- [ ] Failing tests first: order-insensitivity (shuffled pairs → byte-identical `canonical()`), transcribed from `tests/test_econ_cycle.py`; seed pin: `CycleContext{seed:0}.derive_seed("econ-cycle")` equals Python's value for cycle id `econ-cycle-{now}-{seq}`; `py_repr` fixture parity.
- [ ] Implement; commit.

---

### Task 9 (Wave 4, GATE): Conformance corpus vendoring + Rust fixture-runner

**Mirrors the harness shape of:** `tools/conformance/validate_fixtures.py` (schema gate: every payload declares `schema_name`/`schema_version`, unknown = failure) + `tools/conformance/generate_scenarios.py` (replay semantics: wire shapes `_arb_wire` = `{ordered_intent_ids, rejected:[{intent_id, reason_code, conflicting_key}]}`, `_decision_wire` = `{authorized, reason_code}`, `_env()` construction, pinned `NOW = 1_752_400_000`) and `tests/test_conformance_corpus.py` (40 dirs, no documented gaps, every case has inputs+expected).

**Files:**
- Create: `fixtures/conformance/scenarios/**` — byte-for-byte copy of `~/bin/cl_revenue_ops-port/tests/conformance/scenarios/` (all 40 dirs; `cp -r`, commit; these are the SAME corpus files, vendored)
- Create: `crates/revops-econ/tests/conformance.rs`

**Runner contract:**
- [ ] **Step 1 (failing test skeleton):** test `corpus_is_byte_identical_to_source` — when `~/bin/cl_revenue_ops-port/tests/conformance/scenarios` exists (or `REVOPS_PY_CORPUS` is set), every vendored file's bytes equal the source's (guards drift both directions); test `forty_scenarios_present_and_schema_gated` — 40 dirs, every `*.json` parses, `schema_name`∈{`conformance_case`, `ledger_event`, `ledger_projection`} with known versions, every case has non-null `inputs` and non-empty `expected` (mirror of the Python validator's role, typed-struct form).
- [ ] **Step 2: replay dispatch.** `const NOW: i64 = 1_752_400_000;` (use `inputs.now` when present, e.g. s30). Replayed byte-for-byte in Phase 2 — comparison is `canonical_json(produced) == canonical_json(expected)`:
  - **arbitration** (18, 20, 21, 31, 35, 38): `from_wire` each `inputs.intents` entry; also re-derive via `make_intent` from the wire fields and assert `intent_id`/`idempotency_key` match the input bytes (cross-language key parity); `arbitrate(now, inputs.extended_rules.unwrap_or(false))`; build the `_arb_wire` shape (note `conflicting_key` carries the DETAIL string — port strings exactly).
  - **authorization** (22, 29, 30): stub facade per inputs (`reserve_delegate_grants`, `authority_level` "observe" vs required "capital" for s29's gated leg, ungated leg with `authority_check: None`); compare `_decision_wire` shapes (s29 compares the two sub-decision dicts; the prose `invariant` field is copied through from expected, not computed).
  - **ledger** (25, 26) + **production_capture** (40): tempdir `EconLedger`, append `inputs.events`/lifecycle, `replay()`, compare projections (s40 additionally replays `expected-ledger-events.json` events and compares against `expected-projections.json`).
  - **failure_mode** (27): ledger with `execution_started` at `NOW - inputs.age_seconds`; `reconcile(now=NOW, stale_after=inputs.stale_after_seconds)`; derive `{resolvable_as_db_missing: any(kind=="db_missing"), quarantine_when_stale: any(kind=="unknown_outcome" && resolution.is_none())}`.
  - **intent_semantics** (32, 33, 34) and **determinism** (36, 37): direct calls (`Msat::from_checked(1i128<<63)` / `Msat::new(-1)` → "EconArithmeticError" classifier strings; floor/ceil arrays; expiry probes; canonical insertion-order independence; twice-built `_env` id equality + exact `intent_id` string).
- [ ] **Step 3: pinned skip list** (explicit, never silent): `const DEFERRED: &[(&str, &str)] = &[("01-…","phase3 classification"), … ]` covering 01–17, 19, 23, 24, 28, 39 with the owning phase (classification/fee_stage/admission/rebalance_mode → Phases 3–5; reservation 23/24 → Phase 3 budget rail; lnplus 28 → Phase 6; 39 prose-only contract). A test asserts every scenario dir is either replayed or in `DEFERRED` — adding scenario 41 breaks the build until triaged.
- [ ] **Step 4:** full `cargo test --workspace` + clippy green; run the Python side once for the record: `python3 tools/conformance/validate_fixtures.py` in the port worktree exits 0. Commit. **This closes the Phase 2 gate.**

---

### Task 10 (Wave 4, parallel, NON-GATING stretch): Shadow hub core

**Mirrors:** `modules/econ_shadow.py` — flag parsing (`'true','1','yes','on'` string tolerance), `_default_ledger_path` (econ_ledger.db beside db_path), `snapshot_ref` TTL cache (300s, ledgered `snapshot_created`), `_journal` + `note_spend_reserved/settled/released` (settle emits `cost_recorded` + `execution_succeeded` + `reservation_released`; reservation_id doubles as intent_id and idempotency_key), `arbitration_registry` (shared, gated by `econ_arbiter_enabled` with live `econ_conflict_rules_extended` provider), `maybe_run_reconciliation` (3600s self-throttle).

**Files:** Create `crates/revops-econ/src/shadow.rs`, `crates/revops-econ/tests/shadow.rs`.

**Contract:** FAIL-OPEN — no method returns `Err` to a caller; internal errors log once at warn then debug and return `None`/no-op. Disabled entirely unless `econ_shadow_enabled`. This task ports the state machine against injected closures (config getter, clock, ledger); the CLN plugin wiring (`revenue-r-econ-snapshot/-cycle/-reconcile` RPCs in `crates/revops`) is Phase 2b, after the diff harness can compare them against Python's `revenue-econ-*` outputs on lnnode.

- [ ] TDD the journal event sequences (settle = 3 events in order), TTL cache boundary (299s hit / 300s rebuild), flag tolerance table, reconcile throttle; commit.

---

## Exit Criteria (Phase 2 gate, from the design spec — NOT compressible)

- [ ] All 18 econ-core conformance scenarios replay byte-identically through `crates/revops-econ/tests/conformance.rs` against the vendored (byte-identical) corpus; 22 deferred scenarios schema-gated with a test-pinned ownership list.
- [ ] Idempotency keys, intent_ids, and arbitration wire output identical to Python for identical inputs regardless of input order (order-shuffle tests in T5/T8/T9).
- [ ] Ledger replay of a copied production `econ_ledger.db` reconstructs identical `LedgerState` (manual verification step: copy lnnode's econ_ledger.db, run a small `#[ignore]`d test against it, compare with Python `replay()` output).
- [ ] Governor decision order (paused → authority → stale → conflict → budget) pinned by tests including fail-closed-on-error.
- [ ] `cargo fmt --check`, `cargo clippy --workspace -- -D warnings`, `cargo test --workspace` green in CI.