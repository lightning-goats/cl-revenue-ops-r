# Rust port — functional parity checklist

**Purpose.** One durable place to answer "is everything ported?" with evidence
rather than recollection. Nothing here may be ticked from memory: each row needs
a Rust module **and** a test, and behaviour-bearing rows need a test that *fails
if the behaviour is reverted*.

**Why this exists.** The 2026-07-27 fee audit found three entry points whose
trigger receipt was recorded and whose effect was never ported. Every automated
gate was green the whole time, because no gate looked at effects. A ported kernel
with no caller is the failure mode this checklist is designed to catch.

Legend: `[x]` ported + tested · `[~]` partial (noted) · `[ ]` not ported ·
**n/a** out of cutover scope.

Status key for "Tripwire": does a test RED if the wiring is removed?

---

## Scope note

The **fee subsystem** is the only cutover scope. Rebalance, capital planner,
Boltz, LN+ swaps and most operator RPCs remain Python-owned and are tracked here
only so the boundary is explicit, not because they gate cutover.

---

## 1. Fee control stack — the cutover scope

Source of truth: `docs/port/port-map.json`, lens "Fee control stack" (16
components). Audited 2026-07-27, evidence in
`/home/sat/agent-tasks/fee-parity-audit-2026-07-27.md`.

| # | Component | Rust | Tests | Tripwire | Status |
|---|---|---|---|---|---|
| 1 | GaussianThompsonState (DTS) | `thompson/` | `thompson_dynamics`, `thompson_sampling`, `posterior` | oracle fixtures | `[x]` |
| 2 | PIDState | `pid.rs` | `tests/pid.rs` | oracle | `[x]` |
| 3 | VegasReflexState | `vegas.rs` | `tests/vegas.rs` | oracle | `[x]` |
| 4 | Cycle orchestrator | `cycle.rs` | `tests/cycle.rs`, `decision_context` | T8b epoch guards ×2 | `[x]` |
| 5 | Market intelligence | `market.rs` | `tests/market.rs` | oracle | `[x]` |
| 6 | Floors + safety rails | `floors.rs` | `tests/floors.rs`, `rails.rs` | oracle | `[x]` |
| 7 | Admission (htlcmax) | `admission.rs` | `tests/admission.rs` | golden fixtures | `[x]` |
| 8 | Drain bias | `drain.rs` | `tests/drain.rs` | oracle | `[x]` |
| 9 | Per-channel state persistence | `state_store.rs`, `thompson/serde.rs` | `state_serde`, `state_roundtrip`, `production_blobs` | round-trip | `[x]` |
| 10 | Fee execution | `execution.rs`, `revops/fee_execution.rs` | `tests/execution.rs`, `fee_execution` | action-surface scan | `[x]` |
| 11 | Governor authorization | `revops-econ` | `governor`, `intents` | fail-closed tests | `[x]` |
| 12 | Failed-forward signal | `dynamics.rs` + `fee_scheduler.rs` | see §2 A1 | mutation-verified | `[x]` |
| 13 | Config | `fee_config.rs`, `config_resolve.rs` | `fee_config`, `config_resolve` | validation tests | `[x]` |
| 14 | DB persistence (fee subset) | `revops-db` | `owner`, `queries`, `budget` | schema tests | `[x]` |
| 15 | DataService (RPC cache) | `fee_evidence.rs` | `fee_evidence`, `read_rpcs` | TTL tests | `[x]` |
| 16 | PolicyManager (fee slice) | `read_policies` | covered in `cycle`/`fee_evidence` | — | `[x]` |

Plus byte-exact strict replay: `replay` (30) + `replay_wire` (17). `[x]`

---

## 2. Entry points — where the 2026-07-27 audit found the gap

A ported kernel is not a ported feature. These are the notification/hook paths
that mutate fee state.

### A1 — failed-forward posterior nudge (py `record_failed_forward`:9179)

- [x] kernel: `is_fee_relevant_failure`, `failed_forward_nudge_weight`,
      `failed_forward_implied_fee`, `record_posterior_nudge` — oracle-tested
      against `update/failed_forward.json`
- [x] **producer** `notify::failed_forward_signal` — status gate, OUTGOING-only
      (DTS-4a), fee-relevance gate (DTS-4b), `in_msat` amount, event timestamp
      — 6 tests incl. a settled-forward control
- [x] **effect** `CycleOwner::apply_failure_nudge` — guard chain + nudge +
      `last_failure_nudge_ts`
- [x] cooldowns: gossip-settle 3600s, rate limit 1800s, measured from **event**
      time (carried in the message, not the dispatch clock)
- [x] never-first-evidence rule (absence of `fee_states` entry ⇒ skip)
- [x] **scheduler-side behaviour tests** — 5 tests driving each guard through
      `handle_failed_forward`, incl. both cooldown boundaries (off-by-one
      checked on each side)
- [x] **revert tripwire** — MUTATION-VERIFIED: reverting the effect to
      recording-only reds 3 tests; file restored byte-exact (sha256)
- [ ] `note_fee_applied` called from Rust's own broadcast path (inert until
      cutover; today nothing applies fees, so the gossip-settle window never
      opens — matches Python only post-cutover)

### A2 — peer policy change (py `_handle_policy_change`:7871)

**Audit correction (2026-07-27):** the EFFECT is already ported. My first pass
called this "receipt only, no kernel"; in fact `CycleOwner::handle_policy_changed`
already calls `policy_changed` -> `revops_fees::cycle::handle_policy_change`,
which wakes the peer's channels. Only the producer is missing.

- [x] effect: wake every channel with that peer (`handle_policy_change`)
- [x] trigger receipt + bounded-queue/backpressure handling
- [x] **producer** `CycleOwner::detect_policy_changes` — remembers
      `peer_policies.updated_at` per peer and fires on ADVANCE, so the change is
      detected whoever made it. Works before AND after cutover, unlike an
      RPC-side hook (Python owns the policy RPC today). First sighting is a
      baseline only: a restart is not a policy change.
- [x] tests + **revert tripwire** — MUTATION-VERIFIED: removing the producer
      call reds `an_advanced_policy_updated_at_produces_a_policy_change`.
      Controls: unchanged policy over 3 cycles stays silent; a REGRESSED
      `updated_at` (clock skew, restored backup) is not a change.

### A3 — new-channel initial fee (py `set_initial_fee`:8584)

**Audit refinement (2026-07-27):** A3 is NOT one job. It splits into two halves
with very different risk and very different timing, and only one of them can be
done before cutover.

**A3a — persistent prior seed (audit F5). Shadow-safe, do this first.**
Pure state mutation, exactly the class A1 is in: on a new channel with a network
gossip prior, seed the PERSISTENT thompson state's `prior_mean_fee` /
`prior_std_fee` and record one durable nudge at
`INITIAL_PRIOR_NUDGE_WEIGHT` (0.3, already ported at `market.rs:331`).
Python's own audit note quantifies the defect when this is missing: the
persistent state stays at the default prior (200/100), so the first regular fee
cycle samples from the default and *walks the fee away from the best available
evidence by up to ~460 ppm/cycle*.
- [ ] seed persistent prior + durable nudge on new-channel observation
- [ ] tests + revert tripwire

**A3b — the initial broadcast. LIVE-ONLY, structurally blocked in shadow.**
Ends in `set_channel_fee`, i.e. a real `setchannel`. Rust must NOT broadcast in
shadow, so this path cannot be exercised before cutover; the shadow-correct
behaviour is to compute and RECORD the would-be initial fee, never send it.
- [ ] producer: `channel_state_changed` → CHANNELD_NORMAL (today `notify.rs` is
      deliberately closure-events-only)
- [ ] channel resolution via `listpeerchannels` (match by SCID or funding
      channel_id; single-NORMAL-channel fallback)
- [ ] decision branches: PASSIVE skip / STATIC target / DYNAMIC prior sample
- [ ] `_select_best_fee_prior` → `_get_network_fee_prior` (an UNCACHED
      `listchannels` RPC — must stay off the per-cycle locked path)
- [ ] shadow: record the would-be fee; live: broadcast through the guarded
      adapter only
- [ ] tests + revert tripwire

### Already wired

- [x] `FixedInterval` / flush-triggered / `RunCycleNow`
- [x] `WakeAll` (`revenue-r-fee-wake`)
- [x] `VegasSpike` — in-cycle wake parity fixed under task 39
- [x] `ForwardEvent` (settled) — recording-only **by design**; settled forwards
      feed revenue via ingested forwards, not a hook-side posterior write

---

## 3. Full-plugin audit — all 9 lenses (2026-07-27)

Method per §5. Every row was checked by locating the Rust module **and** its
tests, not by recollection. Lens 1 (fee stack) is §1 above.

**Headline numbers.** Python ~58k LOC / **69 operator RPCs**. Rust ~55k LOC
across 8 crates / **9 RPCs**. The line counts are close; the RPC surface is
**13%**. That gap is the honest shape of the port: the *decision kernels* are
broadly ported, the *operator surface and the capital-deploying subsystems* are
not.

### Lens 0 — CLN plugin entrypoint (10,378 LOC)

| Component | Rust | Status |
|---|---|---|
| plugin-bootstrap-and-init | `main.rs` | `[x]` |
| threadsafe-rpc-proxy | `revops-rpc` (timeout guard) + `cln-rpc` | `[x]` |
| option-registration-and-config | `options_table.rs`, `config_resolve.rs` (126 options in manifest) | `[x]` |
| background-scheduler-loops | `fee_scheduler.rs` (fee loop only) | `[~]` fee loop only; flow/rebalance/planner/boltz loops absent |
| core-cycle-functions | `fee_scheduler.rs`, `revops-fees/cycle.rs` | `[~]` fee cycle only |
| **rpc-method-surface-core** | `rpc_status/dashboard/history/report.rs` | `[ ]` **9 of 69** |
| notification-subscriptions | `notify.rs` — forward_event, connect, disconnect, channel_state_changed | `[~]` all 4 subscribed; `channel_state_changed` deliberately narrower (closure events only, see A3b) |
| spend-budget-and-capex-rpcs | — | `[ ]` |
| boltz-swap-rpcs-and-auto-cycle | — | `[ ]` |

### Lens 2 — Rebalance stack

Structurally the most complete non-fee port: `revops-rebalance` is 8,167 LOC and
maps 1:1 to the Python components.

| Component | Rust | Tests | Status |
|---|---|---|---|
| EVRebalancer facade + JobManager | `facade.rs` | `facade` | `[x]` |
| RebalanceEngineV2 orchestrator | `engine.rs` | `engine` | `[x]` |
| RebalancePlanner + pair types | `planner.rs`, `types.rs` | `planner` | `[x]` |
| RebalanceRouterV3 (askrene) | `router.rs` | `router` | `[x]` |
| NativeRouteExecutor | `executor.rs` | `executor` | `[x]` |
| Modes + route policy tables | `modes.rs`, `route_policy.rs` | `modes` | `[x]` |
| SegmentObservationStore | `segstore.rs` | — | `[~]` no dedicated test file |
| Defibrillator diagnostic | `defib.rs` | — | `[~]` no dedicated test file |
| — | `cooldowns.rs`, `ev.rs`, `errors.rs` | `cooldowns`, `ev` | `[x]` |

**Not wired to production.** No rebalance RPC, no rebalance loop, and no
sendpay call site in the plugin — the same "kernel ported, no caller" shape that
A1–A3 had in the fee stack. Rebalance moves real sats, so wiring it is a
tier-1 project in its own right, not a follow-up.

### Lens 3 — Governed economics layer

| Component | Rust | Tests | Status |
|---|---|---|---|
| econ_types | `types.rs` | via `conformance` | `[x]` |
| reason_codes | `reason.rs` | `conformance` | `[x]` |
| econ_snapshot | `snapshot.rs` | `snapshot` | `[x]` |
| cycle_context | `context.rs` | via `cycle` | `[x]` |
| econ_intents | `intents.rs` | `intents` | `[x]` |
| econ_arbiter | `arbiter.rs` | `arbiter` (incl. 21+ scenarios) | `[x]` |
| governor_facade | `governor.rs` | `governor` | `[x]` |
| econ_ledger | `ledger.rs` | `ledger` | `[x]` |
| econ_reconcile | `reconcile.rs` | `reconcile` | `[x]` |
| econ_ev | `ev.rs` | via `conformance` | `[x]` |
| econ_cycle | `cycle.rs` | `cycle` | `[x]` |
| econ_shadow | `shadow.rs` | `shadow` | `[x]` |
| risk_profiles | `revops-fees/profiles.rs` | via `fee_config` | `[x]` |
| **governed execution call sites** | fee path only | `[~]` | fee intents wired; rebalance/boltz/planner intents exist as arbiter POLICY STRINGS with no producer |

### Lens 4 — Capital allocation — **essentially unported**

| Component | Rust | Status |
|---|---|---|
| CapexBudgetEngine | none | `[ ]` |
| CapacityPlanner (~4200 LOC) | none | `[ ]` |
| BoltzCliManager (~2670 LOC) | `revops-boltz` (kernels only) | `[~]` |
| BoltzAutoCycle (~1400 LOC) | `revops-boltz/autocycle.rs` (kernels only) | `[~]` |
| LNPlusSwapAutomation (~2099 LOC) | none | `[ ]` |

Verified by search: `boltz` and `lnplus` appear in Rust **only** as arbiter
policy strings, budget-bucket names, config options, and one
`lnplus_contract_protection` helper in `revops-analytics/protection.rs`. There
is no manager, no planner, no automation. ~10,400 Python LOC with no Rust
counterpart. This is the largest single parity gap in the plugin.

**Boltz update 2026-07-27.** `revops-boltz` landed: 2,698 src LOC, 124 tests,
clippy clean. Verified independently before merge (LOC, test counts, and the
absence of any live-call site re-checked — not taken on report).

Ported as PURE kernels: address validation, fee estimation with the E-4.4
anti-double-count guard, swap-state classification (incl. the "abandoned
contains done but isn't completed" trap), journal prune/merge/idempotency,
budget aggregation + atomic pre-create reservation, and the autocycle
mode/error/cooldown state machine. "Cooldown not burned on dry-run" and "budget
blocks don't poison stats" are encoded as TYPES with tests that fail if
reverted. The known ambiguous-outcome sites (subprocess timeout on create, py
`boltz_manager.py:444`; raw-text refund/claim `:2461`/`:2474`; balance-cycle
fallthrough `cl-revenue-ops.py:10208`) are explicit `Unknown`/`Unverified`
variants so they cannot collapse into "definitely succeeded".

NOT ported — which is why this is `[~]` and not `[x]`: per-command `boltzcli`
argv glue, CLN first-hop pinning / external-pay, the autocycle plan BUILDERS
(they depend on `CapacityPlanner`, unported), and governor-facade integration.

**It has no caller.** Recorded here explicitly rather than left for a later
audit. Wiring needs a subprocess `BoltzCli` adapter, `CapexBudgetEngine`, and a
Boltz loop/RPC surface — none of which exist yet.

Note carried from the fee audit: the `lnplus_obligation` selector exists as pure
fns with **no production feed**; a future porter must include opening-state rows
(manual swaps stick in `opening`).

### Lens 5 — Database layer

| Component | Rust | Status |
|---|---|---|
| connection-and-threading | `revops-db/actor.rs`, `owner.rs`, `lib.rs` | `[x]` |
| schema-init-and-migrations | `fee_runway.rs`, `notifications.rs` (Rust-owned tables); adopts production DB read-only | `[~]` Rust never migrates the production schema — by design |
| input-validation-helpers | `revops-core/msat.rs`, `scid.rs` | `[x]` |
| channel-state-and-kalman-store | `queries.rs` + `revops-analytics/kalman.rs` | `[~]` read-only |
| fee-strategy-and-audit | `queries.rs`, `fee_runway.rs` | `[x]` |
| rebalance-history-and-failure-tracking | partial in `queries.rs` | `[~]` |
| budget-reservations-and-spend-ledger | `budget.rs` (+ concurrency & prod-copy tests) | `[x]` |
| forwards-ingestion-and-pruning | `notifications.rs`, `hydration.rs` | `[~]` ingestion yes; **pruning/retention not implemented** |
| revenue-pnl-analytics | `queries.rs`, `revops-analytics/profitability.rs` | `[~]` read paths |
| costs-closures-and-lifetime-accounting | partial | `[~]` |
| peer-reputation-and-connection-history | `notifications.rs` events only | `[~]` |
| config-planner-lnplus-policies | `read_policies` (peer policies only) | `[~]` |

### Lens 6 — Configuration system

| Component | Rust | Tests | Status |
|---|---|---|---|
| Config dataclass + validation tables | `config_types.rs`, `fee_config.rs` | `fee_config`, `config` | `[x]` |
| Startup override loading + risk profiles | `config_resolve.rs`, `profiles.rs` | `config_resolve` | `[x]` |
| Runtime update path (`revenue-config` RPC) | `rpc` config get/set | `config` | `[~]` fee-relevant keys |
| ConfigSnapshot (immutable cycle view) | `FeeCfgSnapshot` | `fee_config` | `[x]` |
| Plugin option registration + setconfig | `options_table.rs`, `main.rs` (126 options) | `manifest` | `[x]` |
| Chain-cost floor + liquidity buckets | `floors.rs` | `floors` | `[x]` |
| Econ artifact JSON schemas | `revops-econ` | `conformance` | `[x]` |

### Lens 8 — RPC proxy, data service, utils

| Component | Rust | Tests | Status |
|---|---|---|---|
| ThreadSafeRpcProxy + socket-timeout guard | `revops-rpc` | `timeout` | `[x]` |
| DataService (tiered RPC cache) | `fee_evidence.rs` prefetch | `fee_evidence`, `read_rpcs` | `[x]` |
| utils (msat/base-unit helpers) | `revops-core/msat.rs` | `rounding_parity` | `[x]` |
| Daemon loop scheduler + heartbeat | `fee_scheduler.rs` | `fee_scheduler` | `[~]` fee loop only |
| DB connection layer | `revops-db` | `actor_wal` | `[x]` |
| Test harness | fixtures + `tempfile` | throughout | `[x]` |

### Lens 7 — architecture/invariants (meta-map, overlaps the above)

`profitability_flow_analysis` (~5300 LOC) → `revops-analytics` (4,599 LOC:
classification, demand_flow, flow, growth, kalman, policy, profitability,
protection, telemetry) — all with test files. `[x]` as kernels, `[~]` as wired
surfaces (no `revenue-profitability` / `revenue-analyze` RPC in Rust).

---

## 3b. What "full functional parity" actually requires

Ranked by size, from this audit:

1. **Capital allocation (~10,400 LOC)** — capacity planner, capex budget, Boltz
   manager + auto-cycle, LN+ swap automation. Nothing exists.
2. **Operator RPC surface (60 of 69 methods)** — everything except fee/status.
3. **Wiring the ported-but-uncalled kernels** — rebalance (8,167 LOC ported, no
   loop, no RPC, no sendpay call site) and the non-fee governed-execution call
   sites. Same shape as A1–A3, at much larger scale and touching real payments.
4. **Retention/pruning** — forwards pruning and the 8 runway tables.
5. **Remaining fee-path items** — A3a, A3b, `note_fee_applied`.

**None of 1–4 blocks the fee cutover**, which is scoped to the fee subsystem and
leaves Python owning everything else. They block "Rust replaces the plugin".
Those are different programmes and should not be conflated.

## 4. Cross-cutting invariants (must stay true at every tick)

- [x] one mutating RPC call site (`fee_execution.rs:808`, `setchannel`)
- [x] one `ClnFeeBroadcaster` construction site under `LiveAuthority` only
- [x] production DB opened read-only
- [x] pre-decision epoch contract (`993632d`) — 2 kernel guards
- [x] canonical-JSON / idempotency-key byte parity
- [x] `v2_state_json` lossless round-trip incl. legacy layouts
- [ ] retention/pruning for the 8 runway tables (needed before long-lived shadow)
- [ ] `store_budget()` floor vs `BUSY_TIMEOUT_MS`
- [ ] arm re-mint closure (before LIVE, not before shadow)
- [ ] two genuinely separate `PythonAuthorityClient` fetches in `authorize`

---

## 5. Method for the remaining full-plugin audit

For each remaining lens map in `docs/port/port-map.json` (9 total; fee stack
done):

1. enumerate its components;
2. locate the Rust module **and** its tests;
3. enumerate that subsystem's **entry points** separately from its kernels —
   this is what caught A1–A3;
4. for anything behaviour-bearing, confirm a revert tripwire exists;
5. record the result as a section here with evidence paths.

Remaining lenses: plugin entrypoint/RPC surface · rebalance · governed economics
· capital allocation · database layer · configuration · architecture-invariants ·
rpc-proxy/data-service. Most are **n/a for cutover** but in scope for "full
functional parity" as a program.
