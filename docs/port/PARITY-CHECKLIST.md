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

## 3. Out of cutover scope (Python-owned; listed for boundary clarity)

- **n/a** rebalance stack (candidate selection → askrene → sendpay → budget)
- **n/a** capital allocation / channel open-close planner
- **n/a** Boltz swap cycles
- **n/a** LN+ swap automation — note: `lnplus_obligation` selector exists in
  Rust as pure fns with **no production feed**; a future porter must include
  opening-state rows (manual swaps stick in `opening`)
- **n/a** operator RPC surface (36+ handlers) beyond the fee/status subset
- **n/a** econ shadow/reconcile beyond the fee ledger slice

---

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
