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

**Superseded 2026-07-27.** The former "fee subsystem is the only cutover scope"
note below is implementation evidence, not the current programme boundary. The
operator-approved design in
`docs/superpowers/specs/2026-07-27-whole-plugin-rust-cutover-design.md` sets the
scope to the **whole plugin**: fee decisions and execution; governed economics,
budget, settlement, reconciliation; profitability, policy, configuration, and
operator RPCs; rebalance planning and execution; capacity planning, opens,
closes, defibrillation; Boltz management and autocycles; LN+ evaluation,
lifecycle, opening, withdrawal, rating, reconciliation; and every background
loop, notification producer, and status surface those features need. Python
remains the sole mutation authority until one coordinated whole-plugin cutover
(design §"Non-negotiable invariants" 1-2); no subsystem earns mutation
authority early merely by being individually complete.

That design also fixes the **honest progress model** this checklist must use
going forward: `[x]`/`[~]`/`[ ]` alone conflate five different levels of
evidence. Report against the five explicit states instead — **compiled**
(module declared + built), **reachable** (RPC registered / notification
producer connected / loop spawned), **effective** (the reachable caller
invokes the intended kernel and a revert-discriminating test reds if removed),
**transport-proven** (exercised against a sandboxed fake external boundary),
and **promotion-ready** (independent review + required shadow/runway evidence,
zero unauthorized mutation calls). **A row may not be marked complete, and
none of the four higher states may be claimed, from source presence or LOC
alone** — only from the specific test/evidence named for that state.

**Task 55 revision-2 correction (2026-07-28): prior revisions still
misclassified three Rust-only methods as Python-equivalent. Exact-name
comparison against Python's 69 registered names established the pre-Task-56
surface as 20 total Rust methods, 16 Python-equivalent, and four Rust-only:
`revenue-ping`, `revops-fee-runway-status`, `revenue-fee-wake`, and
`revenue-rebalance-plan`. Task 56 adds four genuinely Python-equivalent planner
read RPCs, and Task 61 adds four LN+ operator RPCs, so the current guarded
surface is **28 total / 24 of 69 Python-equivalent** (≈35%).**

- **Total Rust RPC methods registered:** **28**, guarded by
  `crates/revops/tests/manifest.rs`'s exact manifest count. Four are the
  Rust-only operational methods listed above and must never count toward the
  69-method Python denominator.
- **Python-equivalent RPCs registered in Rust:** **24 of 69**. The 45 absent
  exact Python names remain the honest pre-cutover work queue; source-only
  builders do not count.

**Baseline (design doc snapshot, `main` @ `0eba911`):** 1,986 passing tests,
27 RPC builders compiled. Its claimed 9-of-69 registered-RPC count is retained
as historical provenance but superseded by Task 55's exact-name inventory; the
pre-Task-49 equivalent count was 6 (16 current at Task 55 minus Task 49's ten).

**Measured at Task 49 (Wave 2 / RPC Batch A), `main` @ `650d832` +1 commit:**
27 `rpc_*` builder modules compiled (unchanged — all ten Batch A modules were
already declared in `lib.rs` before this task; see `crates/revops/RPC_BATCH_A.md`),
**20 total Rust RPC methods** registered in `crates/revops/src/main.rs` — of
which **16 are Python-equivalent** (up from 6); the four Rust-only methods are
listed in the Task 55 correction above. The
exact total-20 count is a manifest-test GUARD,
`crates/revops/tests/manifest.rs`'s `assert_eq!(...len(), 20, ...)`, that
reds on any unannounced addition/removal.
See "Task 49 — Wave 2 reachability" under Lens 0 below for the per-RPC detail;
this task made ten Python-equivalent entry points **reachable**, no more.

**Measured at Task 52 refresh (`main` @ `546238b`, 2026-07-27):** 2,253
passing workspace tests, 0 failed (counted by summing every `test result:`
line of `cargo test --workspace`, not taken from a report). RPC surface
UNCHANGED since Task 49: **20 total / 16 Python-equivalent** registered
methods — `main.rs` contains exactly 20 `.rpcmethod(` sites and the
`manifest.rs` guard still asserts total 20 (`manifest.rs:343-355`). New
`rpc_*` builder modules that exist as source but are NOT registered
(`rpc_planner_candidates.rs`, `rpc_capex_status.rs`, `rpc_lnplus_status.rs`)
are **compiled** only in the five-state model and add nothing to either
count. Merged since the sections below were last written: Task 44 A3
(`accee49` — new-channel initial fee, shadow-only, independently reviewed;
§2 A3 below), Task 47 CapacityPlanner orchestration (`650d832`), the LN+
wiring layer (`4cece2c`), Tasks 49+50 (RPC Batch A + corrections, already
reflected above), and Task 54's Boltz subprocess transport proof
(`546238b`). Lens-4 and §2 corrections below carry the detail.

The original fee-subsystem section (§1 below) remains accurate as the record of
what was audited for the *fee cutover specifically* on 2026-07-27 and is kept
verbatim; treat "cutover scope" language inside it as historical, superseded by
the whole-plugin scope above.

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

**CLOSED for shadow by Task 44 A3 (`main` @ `accee49`, 2026-07-27),
independently reviewed (hexmem task 44, review pass at the pre-integration
identical tree `dca2098`).** The A3a/A3b split below is kept as the honest
historical record of how the work was scoped; every checkbox is now marked
from the specific evidence named, not from source presence.

**A3a — persistent prior seed. DONE.**
Python's own audit note quantifies the defect when this is missing: the
persistent state stays at the default prior (200/100), so the first regular fee
cycle samples from the default and *walks the fee away from the best available
evidence by up to ~460 ppm/cycle*.
- [x] seed persistent prior + durable nudge on new-channel observation —
      `dynamic_initial_fee` (`fee_scheduler.rs`) seeds the SEPARATE persistent
      `ChannelFeeState` with the gossip prior mean/std and exactly one
      `record_posterior_nudge` at `INITIAL_PRIOR_NUDGE_WEIGHT` (0.3,
      `market.rs:331`) at the EVENT timestamp, while sampling a fresh
      throwaway state (py 8711-8765's load-bearing split)
- [x] tests + revert tripwire —
      `dynamic_with_gossip_seeds_persistent_prior_and_nudge`;
      `throwaway_and_persistent_states_diverge_and_throwaway_wins` (scripted
      entropy proves the sample comes from the throwaway, not the nudged
      persistent state); a fee-state-only insert is not durable under the
      serializer, so the commit always writes the complete fee+cycle pair

**A3b — producer → decision → recorded would-be broadcast. Shadow half DONE;
live dispatch remains structurally blocked (correctly).**
- [x] producer: `notify::new_channel_signal` — exact 4-state opening →
      `CHANNELD_NORMAL` matrix (py 7153-7165), nested/flat envelopes, pure
      parse (no RPC/DB/RNG); `main.rs` routes it through async
      `prepare_new_channel` to a prepared owner message
- [x] channel resolution via `listpeerchannels` —
      `fee_evidence::resolve_new_channel`: normalize `:`→`x`, exact
      SCID/funding-`channel_id` match, exactly-one-NORMAL fallback,
      multi-NORMAL ambiguity REFUSES (6 resolution tests)
- [x] decision branches: PASSIVE skip / STATIC target / DYNAMIC prior sample —
      `decide_initial_fee`, including STATIC-without-target falling through to
      DYNAMIC; reason identities `channel_open`/`policy_static` exact
- [x] `_select_best_fee_prior` → network prior — reuses the already-ported
      `market::network_fee_prior` off-owner in `prepare_new_channel` (uncached
      `listchannels` prefetch, never on the owner thread)
- [x] shadow: record the would-be fee — prepared action through the SAME
      `RecordingFeeExecutor`/governed boundary as the per-cycle path; receipts
      prefixed `SHADOW MODE, NOT APPLIED`; disposition
      `new_channel_would_broadcast`; zero mutation (action_surface 3/3)
- [x] tests + revert tripwire — end-to-end
      `new_channel_end_to_end_commits_atomically_and_survives_restart` with a
      mutation demonstration (receipt-only owner handler reds it); atomic
      commit with generation CAS (`commit_fee_cycle_guarded`) proven against
      BOTH interleaving schedules (stale commit refused; late callback never
      installs over a newer owner epoch, memory == DB); event-key idempotency
      across restart; strict fresh-`listconfigs` refusal (F8); durable typed
      refusals; owner never blocks on the store (wedged-store proof); T8b
      epoch guards byte-identical and green
- [ ] live: broadcast through the guarded adapter only — STILL OPEN, and
      structurally blocked as designed: the scheduler starts only in
      autonomous shadow and `State::live_broadcaster` is unused by it; live
      A3 dispatch must arrive with the whole-plugin live scheduler through
      `LiveBatchAuthorization` + `ClnFeeBroadcaster`, with no second
      `setchannel` call site

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

**Headline numbers (as audited 2026-07-27, before Task 49).** Python ~58k LOC /
**69 operator RPCs**. Rust ~55k LOC across 8 crates / **9 RPCs**. The line
counts are close; the RPC surface was **13%**. That gap is the honest shape of
the port: the *decision kernels* are broadly ported, the *operator surface and
the capital-deploying subsystems* are not.

**Updated after Task 61 (LN+ runtime and operator quartet).** Registered Rust
RPC methods: **28 total**; of those, **24 of 69 Python-equivalent RPCs (≈35%)**
— Task 49 made ten Python-equivalent builders reachable, Task 56 made four
planner read builders reachable against real DB/config evidence, and Task 61
made four LN+ operator methods reachable through the real LN+ owner. The four
Rust-only
methods (`revenue-ping`, `revops-fee-runway-status`, `revenue-fee-wake`,
`revenue-rebalance-plan`) are excluded from the 69-method denominator.

The Task 49 milestone itself was **20 total / 16 Python-equivalent** — ten more
Python-equivalent builders reachable (see "Task 49 — reachability" under Lens
0 below). This is a reachability count only, not a
completeness claim: several of the ten (profitability, analyze,
capacity-report, econ-snapshot, most of health) return an explicit
`not_yet_ported` in-band marker (Task 50's F1-F5 fix) because their live
evidence pipelines are unported — registering the RPC did not fabricate the
missing evidence.

### Lens 0 — CLN plugin entrypoint (10,378 LOC)

| Component | Rust | Status |
|---|---|---|
| plugin-bootstrap-and-init | `main.rs` | `[x]` |
| threadsafe-rpc-proxy | `revops-rpc` (timeout guard) + `cln-rpc` | `[x]` |
| option-registration-and-config | `options_table.rs`, `config_resolve.rs` (126 options in manifest) | `[x]` |
| background-scheduler-loops | `runtime.rs`, `loop_health.rs`, `fee_scheduler.rs` | `[~]` real fee pass plus the Task 61 LN+ watcher owner are reachable; LN+ evaluation waits on Task 62, while rebalance/planner/Boltz remain durably `not_wired` with no no-op owner |
| core-cycle-functions | `fee_scheduler.rs`, `revops-fees/cycle.rs` | `[~]` fee cycle only |
| **rpc-method-surface-core** | `rpc_status/dashboard/history/report.rs` + Task 49's ten Batch A builders + Task 56's four planner reads + Task 61's four LN+ methods | `[~]` **24 of 69 Python-equivalent registered**, **28 total Rust RPC methods** (four Rust-only); measured via `crates/revops/tests/manifest.rs`'s method-count guard and distinctive-row/owner-ack handler tests |
| notification-subscriptions | `notify.rs` — forward_event, connect, disconnect, channel_state_changed | `[x]` all 4 subscribed (Python subscribes to exactly the same 4); `channel_state_changed` now parses BOTH closure events and the opening→NORMAL matrix (Task 44 A3 — the "closure events only" narrowing is gone); per-notification EFFECT parity is tracked in §2, not by this row |
| spend-budget-and-capex-rpcs | `revenue-r-spend-ledger` (Task 49) | `[~]` spend-ledger reads reachable; capex RPCs not registered |
| boltz-swap-rpcs-and-auto-cycle | — | `[ ]` |

#### Task 49 (Wave 2 / RPC Batch A) — reachability, 2026-07-27

Ten Python-equivalent read-only response builders (already compiled, per the
baseline above) were registered as real `.rpcmethod()` handlers in `main.rs`,
taking the corrected Python-equivalent registered count from 6 to 16 (20 total
Rust RPC methods, including four Rust-only methods). Per the honest
progress model above, this task's claim is **reachable only** — the manifest
test proves each method name is present in both naming modes and, where a
real `revops-db` query exists, that the handler calls it (a distinctive-row
round-trip test, not just a name check). No row here is marked
effective/transport-proven/promotion-ready; that requires independent review
this checklist entry does not substitute for.

Evidence: `crates/revops/tests/manifest.rs` (`manifest_batch_a_methods_registered_shadow_mode`,
`_canonical_mode`, and the `revenue_r_*` caller-tripwire tests below them),
transcripts in `crates/revops/TASK49-REPORT.md`.

#### Task 56 — planner read RPC reachability, 2026-07-28

Four exact Python method names are now registered in both shadow and canonical
modes: `revenue-planner-candidate-sources`, `revenue-planner-candidates`,
`revenue-planner-history`, and `revenue-planner-status`. The first three use
new read-only `revops-db::queries::{planner_candidates,planner_actions}`
queries; status uses both queries plus the same DB-override > live Python
option > fixture-default precedence as `revenue-config`. Distinctive seeded
candidate/action rows prove the handlers, ordering, limits, null/raw-metadata
shape, source grouping, recent-actions list, and candidate-pool size. Nonempty
positional parameters are refused explicitly. Evidence:
`crates/revops-db/tests/queries.rs` and the `planner_read_*` tests in
`crates/revops/tests/manifest.rs`. State: **compiled, reachable, effective**;
transport-proven and promotion-ready remain pending independent verification.

#### Task 57 — observer runtime and durable loop health, 2026-07-28

The plugin now registers exactly five loop identities in the Rust-owned observer database and exposes them through `revenue-health.loops`. Only the existing real fee owner is instantiated in autonomous-shadow mode; rebalance, planner, LN+, and Boltz have no handles or success-shaped no-op passes and report current-boot `not_wired`. The bounded runtime permits one in-flight pass and eight distinct pending keys, durably counts coalesced/dropped work, begins before execution, and generation-CAS records terminal pass/error state. Terminal generation plus terminal kind, rather than second-resolution timestamp ordering, makes same-second restarts and pass↔error sequences unambiguous. Missing begin/terminal/backpressure persistence suspends fail-closed; terminal-write loss leaves an unmatched durable generation. `AuthorityRuntime::Observer` cannot hold or construct action adapters; the broadcaster exists only in `AuthorityRuntime::Live`. Evidence: `crates/revops-db/tests/loop_health.rs`, `crates/revops/src/runtime_tests.rs`, `crates/revops/tests/{fee_scheduler,action_surface,manifest}.rs`, and `rpc_health.rs` unit tests. State: **fee runtime compiled/reachable/effective in autonomous shadow; durable inventory effective; other four loops not reachable and not effective; no authority transition, deployment, or live call performed.**

#### Task 61 — LN+ runtime and operator surface, 2026-07-28, `9c99d7c`

Task 61 supersedes the Task 57 and Task 52 LN+ reachability statements above:
the real watcher owner now runs behind `LoopId::LnPlus`; evaluation remains
honestly deferred until Task 62 supplies the planner rail. Concrete local-fake
proof covers `revops_lnplus::UreqTransport`, `ClnSigner`, and
`ClnChainAdapter`. The exact Python methods `revenue-lnplus-status`,
`revenue-lnplus-breaker-clear`, `revenue-lnplus-abandon`, and
`revenue-lnplus-backfill` are registered and complete through owner
acknowledgements, including admitted-versus-outcome-unknown handling. Task 61
passed independent review. State: **RPCs compiled/reachable/effective and
transport-proven locally; watcher compiled/reachable/partial; soak and live
promotion remain pending; no deployment, authority transition, or live external
call was performed.**

#### Task 66 — Python canonical RPC set closure, 2026-07-31, `c4ba670`

The registration gap is CLOSED: `manifest.rs::canonical_mode_registers_
exactly_the_python_rpc_set` is green — the Rust plugin registers exactly
Python's 69 canonical `revenue-*` methods (0 missing, 0 unexpected), every
one through the Task 64 parameter contract (`fixtures/port/rpc_params.json`).
The seven post-compaction slices (`680e0a9`, `0fe82d5`, `28916cf`, `1dd9481`,
`6ac4b22`, `85802ca`, `c4ba670`) added total-cost-budget, spend-reserve/
release-stale, cleanup-closed, econ-reconcile, econ-cycle, capex-status, and
set-fee; each landed RED-first with a per-slice mutation harness (all kills
by TEST failure). Honest state, per `fixtures/port/plugin_inventory.json`
(generator v4, which now parses helper-fn registrations and the
mode-conditional wake-all binding): **69/69 reachable; 15 full / 54 partial;
review passed only for the 12 previously-reviewed names — every Task-66
contract is review-PENDING** (the tier-1 Python review of the 17-commit
queue). `full` and `reviewed` are now SEPARATE axes in the generator
(`FULL_EFFECTIVE_RPCS` vs `REVIEWED_FULL_RPCS`) so a complete contract can
never imply a review that has not happened. Write-shaped surfaces (the nine
core-state mutators, cleanup-closed, set-fee, fee-cycle, rebalance/planner/
Boltz actions) classify `partial` deliberately: their contracts are complete
INCLUDING Python's exact uninitialized/denial arms, but the result-bearing
execution capability stays sealed until Task 69's authority-gated assembly.
Standing safety facts, each e2e-pinned: set-fee is double-sealed (authority
lease denies in every non-live mode AND the manual setter is unassembled);
the econ RPCs touch ONLY the Rust-owned `econ_ledger_dryrun.db` (one shared
filename constant; a swap to Python's production name dies by mutation);
capex-status performs NO datastore push (declared delta per the authority
record above); Python's production stores receive no writes.

**Round-2 correction (P1): the table below is the CURRENT response contract
for each of the ten Batch A RPCs** (i.e. it already reflects the Task 50
correction round's F1-F5/F10/F11 fixes below and this round's mixed-type-tags
fix) — this checklist reports current contracts directly rather than
retaining Task 49's original (pre-correction) shapes as if they were still
current evidence. Task 49's original shapes, which a Python-side adversarial
audit found collided with or fabricated Python's real vocabulary, are kept
verbatim as a clearly-labeled HISTORICAL record (not current evidence) in
"Historical: Task 49's original (pre-Task-50) shapes" immediately after the
Task 50 section below.

| RPC (canonical name) | Evidence wired | Honest gaps / current contract |
|---|---|---|
| `revenue-health` | `queries::pnl_summary` (today/week financials) — real, DB-backed; `db=None` returns the same honest `build_health(now, None, None, None)` shape (round 2, P1), never a top-level error | `annualized_roc_pct` (needs live `listpeerchannels` capacity), channels/rebalancer/budget/planner/top_routes remain `null`+`_gaps`-listed; `loops` is a Rust-owned durable five-identity inventory with wiring, generation/terminal kind, timestamps, errors, and backpressure counters; `fees` is `null`+gap-listed too, but a real fee controller (`revops-fees::cycle::ControllerState`) DOES exist and run — the gap is that its live state isn't plumbed into this handler yet (round 2, P2), not that no controller exists; `boltz` is the honest computed `{"enabled": false}` (no Boltz manager wired), not a gap |
| `revenue-profitability` | none yet | explicit `not_yet_ported` marker on both branches (F3/F4 fix) — no `ChannelProfitability` assembly pipeline exists; never Python's real "No data available" / real fleet-summary shape |
| `revenue-analyze` | none yet | explicit `not_yet_ported` marker (F5 fix, `MetricsLookup::NotWired`) when the pipeline never ran — no `FlowMetrics` assembly exists; distinguished from Python's genuine unknown-channel `{"channel": id, "analysis": null}` (no marker); no-`channel_id` whole-fleet sweep also returns `not_yet_ported` (mutating background job in Python, out of this read-only batch) |
| `revenue-policy` (list/get/find/changes) | `queries::all_policies`/`policy_for_peer`/`policies_by_tag`/`policy_changes_since`/`last_policy_change_timestamp` — real, DB-backed | set/delete/tag/untag/batch refused before any DB access (`policy_action_gate`, proven by a test with no db-path configured); `since`/action normalization match Python exactly (F6/F9); an out-of-i64-range `since` is a loud in-band error (round 2, P1), never a wrapped/saturated value |
| `revenue-list-banned` | `queries::all_policies`, filtered by the `banned` tag — real, DB-backed | none (fully wired); a mixed-type `tags` array (e.g. `["banned", 7]`) no longer drops the peer (round 2, CRITICAL fix) |
| `revenue-list-ignored` | `queries::all_policies`, filtered by strategy/rebalance-mode — real, DB-backed | none (fully wired); DEPRECATED, ported for parity only; a mixed-type `tags` array no longer silently defaults `reason` to `"manual"` (round 2, CRITICAL fix) |
| `revenue-hot-channel-protection-peers` (`list` only) | `queries::hot_channel_protection_override_peers` — real, DB-backed | add/remove/clear refused (DB writes, out of scope) |
| `revenue-capacity-report` | `timestamp` only | Python's EXACT `{"error": "Capacity planner not initialized"}` (1 key, no `timestamp`, F2 fix) — never the success-shaped 6-key stub |
| `revenue-econ-snapshot` | none yet | in-band `{"error": "econ shadow not_yet_ported", ...}` (F1 fix) — never the hardcoded `enabled=false` that could read as a real (possibly false) answer |
| `revenue-spend-ledger` | `queries::spend_ledger_aggregates` + `active_spend_reservations` — real, DB-backed | none (fully wired); `_gaps` is always `[]` per the builder's own contract; an out-of-i64-range `window_hours`/`reservation_limit` is a loud in-band error (round 2, P1), never a wrapped/saturated 1-hour-ledger-shaped success |

#### Task 50 (Wave 2 / RPC Batch A) — Python-semantics correction round, 2026-07-27

A Python-side adversarial audit (11 lettered findings F1-F11, plus per-method
notes) of the exact snippets Task 49 wired verbatim found fabrication-class and
collision-class defects that would ship a wrong answer if left as-is — a
success-shaped response where Python errors, or a shape that collides with one
of Python's own legitimate answers so a caller cannot tell "port not wired" from
"real data". This round FIXED F1-F9 and F11's in-band error convention, plus two
items the supervisor upgraded from declare-only to FIX mid-round (F10, and
Batch-A-scoped positional-parameter rejection). Full per-finding red/green
transcripts: `crates/revops/TASK49-REPORT.md`'s "Task-50 correction round"
section.

| Finding | Method | Fix | Evidence |
|---|---|---|---|
| F1 | econ-snapshot | No `EconShadow`/`econ_shadow_enabled` config surface exists in Rust — stopped hardcoding `enabled=false` (a possible LIE on a node where Python's real config is `true`). Now returns an in-band `{"error": "econ shadow not_yet_ported", ...}` that cannot be read as either a true or false `enabled` answer. | `rpc_econ_snapshot::build_econ_snapshot_not_wired`, `manifest.rs::revenue_r_gap_only_batch_a_methods_stay_honest` |
| F2 | capacity-report | Returns Python's EXACT `{"error": "Capacity planner not initialized"}` (1 key, no `timestamp`, cl-revenue-ops.py:4586-4587) instead of the success-shaped 6-key stub. | `rpc_capacity_report::capacity_planner_not_initialized_error`, same manifest test |
| F3/F4 | profitability | Both branches now carry an in-band `not_yet_ported` marker (`build_profitability_channel_not_wired`/`_summary_not_wired`) instead of reusing Python's real "No data available" / real fleet-summary shape. | `rpc_profitability.rs` unit tests, same manifest test |
| F5 | analyze | `MetricsLookup::NotWired` vs `Ready(Option<&FlowMetrics>)` distinguishes "pipeline never ran" (now carries `"error": "not_yet_ported"`) from Python's genuine unknown-channel `{"channel": id, "analysis": null}` (no marker). | `rpc_analyze::MetricsLookup`, `rpc_analyze.rs` unit tests, same manifest test |
| F6 | policy `changes` | `since` now coerced via `rpc_policy::coerce_since` (Python-truthiness gate, then `python_int`, matching `int(since) if since else 0`); garbage returns the (previously dead-code) `invalid_since_error()`. | `manifest.rs::revenue_r_policy_changes_since_coercion_matches_python` |
| F7 | spend-ledger | `window_hours`/`reservation_limit` coerce via `python_int` (numeric strings, floats truncating, garbage -> in-band error), NO upper clamp on `window_hours` (deliberately unlike `_total_cost_budget_status`'s `[1,168]`); `include_reservations` matches Python truthiness (`bool("false")` is `True`). | `rpc_spend_ledger::parse_window_hours/parse_reservation_limit/parse_include_reservations`, `manifest.rs::revenue_r_spend_ledger_window_hours_and_truthiness_match_python` |
| F8 + H6 | hot-channel-protection-peers | `normalize_action`: `str(action or "list").lower()`, NO `.strip()` (so `"LIST"` succeeds, `""`/`null` default to `list`, `" list"` is unknown). Split `write_action_refused_error` (real write actions: add/remove/clear) from `unknown_action_error` (garbage strings) — previously one conflated message. | `rpc_hot_channel_protection_peers.rs` unit tests, `manifest.rs::revenue_r_hot_channel_protection_peers_action_normalization_matches_python` |
| F9 | policy | `normalize_action(Option<&Value>)`: absent key -> `"list"`; explicit `null`/non-string/falsy -> `""` -> the (already-correct) 9-name unknown-action error. Previously collapsed both cases through `Option<&str>`, so an explicit `action: null` silently succeeded as `list`. | `rpc_policy.rs` unit tests, `manifest.rs::revenue_r_policy_explicit_null_action_is_refused_not_treated_as_list` |
| F11 | health + all ten | Every `?`-propagated DB call across all ten handlers now returns an in-band error instead of a JSON-RPC error envelope. `revenue-health` specifically: a `pnl_summary` failure becomes `financials: {"error": ...}` with the other eight sections intact (previously the WHOLE call failed, losing every section). | `manifest.rs::revenue_r_health_pnl_failure_is_an_in_band_financials_error_not_a_whole_call_failure` |
| "should NOT stay gap" | health | `boltz` section is now the honest `{"enabled": false}` (Python's own true answer with no Boltz manager) instead of `null` + a `_gaps` entry. `fees` (computable from the live `ControllerState`) was scoped SKIP — needs scheduler plumbing into the handler; declared wireable-now-but-not-wired, not fixed this round. | `rpc_health.rs::boltz_section_is_the_honest_enabled_false_shape_not_a_null_gap` |

**Scope-updated FIXES (were declare-only in the original Task 50 brief, upgraded
mid-round by the supervisor):**

- **F10 (list-banned/list-ignored/policy row-drop, security-relevant).** Fixed
  narrowly in `crates/revops-db/src/queries.rs`'s `decode_policy_row`: every
  scalar column now decodes via a lossy `SqlValue`-based accessor
  (`sql_text_or`/`sql_opt_i64`/`sql_opt_f64`) that defaults on NULL/mistyped
  cells instead of `?`-erroring the whole row. A malformed `peer_policies` row
  can no longer silently vanish from `revenue-r-list-banned` (the audit's
  "a banned peer can silently vanish" scenario) — the row is always kept, with
  only the genuinely-malformed COLUMN falling back to a default (`expires_at`
  defaults to `None`, the fail-safe reading for a security-relevant field: a
  garbage expiry keeps the row visible rather than reading as
  already-expired). This is the "Preferred fix: keep-with-defaults" outcome
  the supervisor named, not the loud-error or unregister fallbacks.
  **Round-2 correction (P2): this is a DELIBERATE FAIL-SAFE DIVERGENCE from
  Python, not a "Python-exact" port** — Python's `_row_to_policy`
  (policy_manager.py:384-439) does NOT generally coerce malformed scalar
  column types; it returns `row['peer_id']`/`row['fee_ppm_target']`/
  `row['updated_at']`/the present v2 columns exactly as SQLite stored them,
  unchecked, wrapping ONLY the tags-JSON decode and the two enum conversions
  in try/except-with-default. Coercing every scalar column too (and, as of
  round 2, decoding the `tags` JSON array at the ELEMENT level rather than as
  one typed `Vec<String>` — see below) is a Rust-side strengthening chosen to
  guarantee no row and no valid string tag is ever silently dropped, not a
  claim that Python performs the same coercion.
  Evidence: `crates/revops-db/tests/queries.rs`'s
  `corrupt_scalar_column_is_kept_with_defaults_not_dropped` and
  `corrupt_updated_at_column_defaults_to_zero_row_still_present` (query-level),
  `crates/revops/tests/manifest.rs`'s
  `revenue_r_list_banned_does_not_drop_a_banned_peer_with_a_malformed_column`
  (RPC-level).
- **Round 2 (2026-07-27) — CRITICAL: mixed-type `tags` array, one layer
  below F10.** A re-review found F10's row-level fix did not cover a
  malformed TAGS-ARRAY ELEMENT: valid SQLite JSON like `["banned", 7]` is a
  legal Python list (`"banned" in tags` is still `True` with the non-string
  `7` present), but the pre-round-2 Rust decode parsed the whole tags column
  as `Vec<String>`, which fails on the first non-string element, and
  `.unwrap_or_default()` wiped the ENTIRE array to `[]` — silently erasing a
  real `"banned"` tag and vanishing the peer from `revenue-r-list-banned`,
  recreating F10's exact failure mode one layer down. Fixed in
  `revops-db/src/queries.rs`'s new `decode_tags_json`: parses the column as
  a generic `serde_json::Value` and keeps only the elements that ARE JSON
  strings, dropping non-string elements INDIVIDUALLY rather than wiping the
  whole array. This is also a deliberate fail-safe divergence (Rust's typed
  `Vec<String>` has no slot for a raw non-string member the way Python's
  heterogeneous list does), documented in `decode_tags_json`'s doc comment.
  Evidence: `crates/revops-db/tests/queries.rs`'s
  `mixed_type_tags_array_preserves_valid_string_members_not_dropped_wholesale`
  and `mixed_type_tags_array_preserves_ignored_reason_tag` (query-level, RED
  captured against unmodified `2b3d356` before the fix);
  `crates/revops/tests/manifest.rs`'s
  `revenue_r_list_banned_does_not_drop_a_peer_over_a_mixed_type_tags_array`
  and `revenue_r_list_ignored_preserves_reason_tag_despite_mixed_type_tags_array`
  (RPC-level, same RED-first capture). Full transcripts:
  `crates/revops/TASK49-REPORT.md`'s "Round 2 corrections" section.
- **Batch-A positional-parameter rejection.** `rpc_params::reject_positional_params`
  refuses any NON-EMPTY JSON array param on all ten Batch A handlers with an
  in-band error; an EMPTY array (`lightning-cli`'s own no-argument call shape)
  still means "no params". This is Batch-A-only, NOT the port-wide positional
  binder Python's pyln implements everywhere — see the follow-up item below.
  Evidence: `manifest.rs::revenue_r_batch_a_methods_reject_nonempty_positional_params_empty_array_still_succeeds`.

**Remaining DECLARE-ONLY decisions (per the original Task 50 brief, not
upgraded, not implemented this round):**

- **Full positional-parameter parity (port-wide, decide-once item for the
  supervisor).** Every RPC in this plugin — not just the ten Batch A methods —
  reads named params via `v.get("name")`, which is `None` for a positionally-
  bound array. pyln binds positionally everywhere. This round closed the
  Batch-A-specific hole (refuse rather than silently default), but a caller
  using `lightning-cli <any-other-rust-rpc> <positional-args>` still gets
  silent defaults port-wide. **Follow-up for the supervisor**: decide once
  (implement real positional binding vs. extend the refuse-and-error pattern
  everywhere) rather than each future RPC reinventing its own answer.
- **Python-side read-RPC mutations (§4 of the Task 50 audit) — copied into the
  authority record so the parity harness expects Python-side drift, not
  fixed/changed in Rust:**
  1. Python `revenue-analyze`'s single-channel path writes `channel_states` +
     Kalman state; the whole-fleet path additionally decays/deletes
     `peer_reputation` (destructive UPDATE+DELETE, `database.py:6912-6957`).
  2. Python `revenue-policy get` on an EXPIRED hit purges ALL expired rows
     node-wide (`BEGIN IMMEDIATE`, `policy_manager.py:466-498`) — so Python's
     own `last_change_timestamp` can SHRINK over time; Rust's never does
     (declared, not "fixed" — Rust staying read-only is correct, not a bug).
  3. Python `revenue-health` writes (datastore push, reservation cleanup);
     `revenue-econ-snapshot` takes a writer lock (`BEGIN IMMEDIATE`) and can
     push datastore; `revenue-capacity-report` runs the full mutating Kalman
     pipeline (`analyze_all_channels` + `flow.analyze_all_channels`).
  4. Python's own suppressions, worth knowing but NOT to be replicated as
     virtues: `top_routes` -> `[]` on error; econ budget/profitability
     failures -> silent zeros/UNKNOWN roles; policy `changes` -> `[]` silently
     on one corrupt row (Python's OWN drop-on-corruption behavior for
     `changes` specifically — contrast the Rust `queries::all_policies`/
     `list`/`get`/`find` fix above, which keeps the row; `policy_changes_since`
     reuses `all_policies` under the hood so it inherits the SAME fix, a
     Rust-side improvement over Python here, not a regression to match).
- **Other per-method declare-only items from the audit, not touched this
  round** (unranked, not required for this batch): P3 (policy's tactical-
  action refusal message says "deprecated", which misattributes WHY an
  `internal=true` write doesn't happen in Rust — Rust implements no write path
  to unlock at all); P4 (the policy `get`-purge asymmetry vs. Rust's stable
  `last_change_timestamp`, folded into item 2 above); H8 (hot-channel-
  protection-peers' row decode has no per-row `.ok()` isolation, unlike
  `peer_policies` — one mistyped cell fails the WHOLE call where Python passes
  it through; the F10 fix above did not extend to this table); the
  `find tag=""`/`find tag=<number>`/`get peer_id=""`/newline-peer-id edge
  diffs in §2.6; econ-snapshot's E5-E8 (non-string scid, negative budget sats,
  NaN ratio); analyze's A4/A5 (no init gate in the builder, Unicode-digit/
  trailing-newline regex leniency); health's H9 (the four `annualized_roc_pct`
  capacity semantics, needed before `total_capacity_sats` is ever wired).

#### Historical: Task 49's original (pre-Task-50) response shapes — NOT current evidence

**This subsection is historical record only — do not read any cell below as
the current contract.** The corrected, CURRENT per-RPC table is above, under
"Task 49 (Wave 2 / RPC Batch A) — reachability". Kept verbatim here (round-2
correction, P1: moved out of the current-evidence table rather than left in
place labeled "superseded", which a reader could still mistake for live
evidence) so the record of what Task 49 originally shipped, before the Task
50 audit found it fabrication/collision-prone, is not lost:

- `revenue-health`: `annualized_roc_pct` (needs live `listpeerchannels`
  capacity), and channels/fees/rebalancer/budget/boltz/planner/top_routes/loops
  (no Rust daemon-loop state) — all `null` + `_gaps`-listed. (`boltz` was later
  fixed to the honest `{"enabled": false}`, not a gap — see the Task 50 table.)
- `revenue-profitability`: no `ChannelProfitability` assembly pipeline exists;
  single-channel returned `"No data available"`, no-`channel_id` returned an
  all-zero summary shape, `fee_multiplier` always `null` — these SHAPES
  collided with Python's own legitimate answers (F3/F4).
- `revenue-analyze`: single-channel `analysis` always `null` (no `FlowMetrics`
  assembly) — collided with Python's genuine unknown-channel answer (F5).
- `revenue-capacity-report`: returned a success-shaped 6-key stub with
  `timestamp` only, instead of Python's exact 1-key
  `{"error": "Capacity planner not initialized"}` (F2).
- `revenue-econ-snapshot`: `enabled` hardcoded `false` with no gap marker — a
  possible LIE on any node where Python's real config is `true` (F1).

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
| CapexBudgetEngine | `revops-capital/capex.rs` | `[x]` full port |
| CapacityPlanner (~4200 LOC) | `revops-capital/planner/` (planning half incl. Task 47 orchestration) | `[~]` no execution call sites, no caller |
| BoltzCliManager (~2670 LOC) | `revops-boltz` (kernels + Task 54 subprocess transport) | `[~]` transport-proven, not reachable |
| BoltzAutoCycle (~1400 LOC) | `revops-boltz/autocycle.rs` (kernels only) | `[~]` |
| LNPlusSwapAutomation (~2099 LOC) | `revops-lnplus` + Task 61 owner/adapters/RPCs | `[~]` concrete transport and watcher/operator caller are locally proven; evaluator waits on Task 62 and live promotion/soak remain pending |

**Task 52 refresh correction (2026-07-27): the paragraph this replaces
("`boltz` and `lnplus` appear in Rust only as arbiter policy strings … no
manager, no planner, no automation … ~10,400 Python LOC with no Rust
counterpart") described the tree BEFORE the `revops-boltz`, `revops-lnplus`,
and `revops-capital` crates landed and was left stale here.** Current truth,
measured at `546238b`: the three crates total ~16,900 src LOC and 536 crate
tests (LN+ 6,218 src / 210 tests; Boltz 5,651 src / 222 tests; Capital 5,035
src / 104 tests — counted by `cargo test -p` + `wc`, not recollection). The
largest remaining gap in this lens is therefore NO LONGER absent code — it is
**callers, concrete transports, and governed execution**: no plugin loop
spawns any of them, no capital/LN+/Boltz RPC is registered, and nothing here
can currently touch a node, a wire, or a sat.

**LN+ update 2026-07-27.** `revops-lnplus`: 4,386 src + 2,838 test LOC, 133
tests, clippy clean, no live HTTP/SQL anywhere (verified by search before
merge). All FIVE known Python defects ported as FIXED, each with a control:
finalize now returns `Finalized`/`Deferred` and the watcher can only report what
actually happened; rating idempotency is a structural `http_status==422` +
parsed-errors-dict match — I verified `structural_contains` scans only parsed
values, and the control at `finalize.rs:297` proves a 422 with "already" in
free-text does NOT match; terminal 422 withdrawal shapes classify as
`Withdrawn`/`Advanced` not `Retryable`; a deadline-miss now patches the row to
`failed` so expired swaps stop consuming reserved budget forever; and
`BreakerCause::is_reverifiable()` is true for EXACTLY the two ghost causes and
false for missed deadlines, remote divergence, ambiguous funded-channel matches
and LN+ outages (verified by reading the match arms).
NOT ported as of that update: the real HTTPS + signmessage transport,
`CapacityPlanner` integration (blocked — see below), the Phase-2F governed
reservation path, `revops-db` schema for the LN+ tables, and the concurrency
guards. **No caller.**

**LN+ wiring-layer update (Task 52 refresh, 2026-07-27, `4cece2c`).** Three of
the five NOT-ported items above have since narrowed; measured at `546238b` the
crate is 6,218 src + 4,860 test LOC, 210 tests:
- **Transport:** `http.rs`'s `LnPlusApiClient` now ports the COMPLETE
  `LNPlusClient` logic (endpoints, `_request` size-cap/error/JSON handling,
  `_unwrap_list_envelope`, signmessage auth flow, structured-422 parse) —
  but generic over an `HttpTransport` trait with **no concrete implementation
  shipped, by design**: no HTTP client crate exists anywhere in the workspace
  dependency graph, so "no test may make a live HTTP request" is true by
  construction. Wiring it live means adding `ureq` and a ~15-line trait impl
  (the file's own doc names the exact shape). Five-state: **effective against
  a fake transport; not transport-proven, not reachable.**
- **DB schema:** `sqlite_db.rs`'s `SqliteLnPlusDb` implements the
  `lnplus_swaps`/`lnplus_peers` schema and queries INSIDE the crate
  (superseding the old "blocked on `revops-db` tables" note) and COMPOSES
  with the already-reviewed `revops_db::budget::BudgetDb` for the unified
  budget rail rather than re-implementing it.
- **Lifecycle:** `loop_drivers.rs` carries the evaluator/watcher passes.
- **Still true:** nothing in `crates/revops/src/main.rs` spawns any LN+
  loop or registers any LN+ RPC (`rpc_lnplus_status.rs` is a compiled-only,
  UNREGISTERED builder). **No plugin caller**, and `CapacityPlanner`
  integration + the governed reservation path remain open.

**Capital update 2026-07-27.** `revops-capital`: 2,031 src LOC, 39 tests over
108 fixture scenarios. Methodology worth noting — the fixtures were generated by
`tools/port/gen_*_fixtures.py`, which import and run the REAL unmodified Python
engines and capture their output, rather than hand-derived expectations. That
caught a genuine Python dead-code quirk in `_extract_actual_close_fee_sats`
(its second lookup loop returns `0` on the first missing key, never `None`),
now documented and pinned.
- `CapexBudgetEngine`: FULL port (tiers, ROI clamps, dead-capital/gateway
  efficiency, fleet priority, envelope scale-down, CB-4 fail-closed).
- `CapacityPlanner`: a curated PURE SUBSET only (~2k of ~4.2k LOC) — portfolio
  gate, close-fee, dead-capital stage machine, EV, gates, scoring.
  NOT ported as of that update: `execute_cycle`, the 5 candidate-discovery
  strategies, `_execute_open`/`_close`/`_defibrillation` (real RPC call
  sites), `_identify_winners`/`_losers`, `_score_candidate`. **No caller.**

**CapacityPlanner orchestration update (Task 52 refresh, 2026-07-27; Task 47,
merged at `650d832`).** Most of the NOT-ported list above has since landed as
PURE planning code; measured at `546238b` the crate is 5,035 src LOC / 104
tests. Now present in `revops-capital/src/planner/`: `cycle.rs::plan_cycle`
(the pure planning half of `execute_cycle`, evidence-in/`CyclePlan`-out) and
`discover_peers`; SIX discovery strategies in `discovery.rs`
(`discover_from_winners`/`_neighbors`/`_graph`/`_route_pairs`/`_demand_flow`
plus the Task-47-review `_neighbors_capital_efficiency`);
`winners.rs::identify_winners`; `losers.rs::identify_losers`;
`candidate_score.rs::score_candidate`; plus `sizing`/`recycle`/`dedup`.
**Still NOT ported:** `_execute_open`/`_close`/`_defibrillation` — the real
RPC call sites that spend and move sats — and any loop/RPC caller:
`rpc_planner_candidates.rs` and `rpc_capex_status.rs` exist as compiled-only,
UNREGISTERED builders. Five-state: **compiled + oracle-tested kernels;
nothing reachable.**

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

NOT ported as of that update — which is why this is `[~]` and not `[x]`:
per-command `boltzcli` argv glue, CLN first-hop pinning / external-pay, the
autocycle plan BUILDERS (they depend on `CapacityPlanner`, unported), and
governor-facade integration.

**It has no caller.** Recorded here explicitly rather than left for a later
audit. Wiring needs a subprocess `BoltzCli` adapter, `CapexBudgetEngine`, and a
Boltz loop/RPC surface — none of which exist yet.

**Boltz subprocess-transport update (Task 52 refresh, 2026-07-27; Task 54,
merged at `546238b`).** Measured at `546238b` the crate is 5,651 src LOC /
222 tests. `process.rs`'s `ProcessBoltzCli` (the subprocess `BoltzCli`
adapter named as missing above) is now **transport-proven** under the
five-state model: `tests/process_fake_executable.rs` drives the REAL
spawn/wait/kill path against sandboxed, test-owned fake executables —
argv/datadir propagation, trimmed stdout on exit 0, stderr-preferred /
stdout-fallback errors with exact exit codes, NotFound mapping, configured
and overridden timeouts, timeout kill AND reap of the exact child (PID
evidence, no survivor), no deadlock on large simultaneous stdout+stderr, and
a real `ProcessBoltzCli` create-timeout classifying through the command
layer as the typed ambiguous/`Unknown` outcome (never retryable). The old
`ENTRYPOINTS.md` claim that `run` "is, by the HARD RULES, untested" is
superseded — the rule is now sandbox-only execution, not no-subprocess.
Per its own report: **this proves the transport boundary but does not make
it reachable from the plugin entrypoint** — still no Boltz loop, RPC, or
caller.

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
| Daemon loop scheduler + durable health | `runtime.rs`, `loop_health.rs`, `fee_scheduler.rs` | `runtime`, `loop_health`, `fee_scheduler` | `[~]` bounded real fee pass only; four later subsystem passes remain `not_wired` |
| DB connection layer | `revops-db` | `actor_wal` | `[x]` |
| Test harness | fixtures + `tempfile` | throughout | `[x]` |

### Lens 7 — architecture/invariants (meta-map, overlaps the above)

`profitability_flow_analysis` (~5300 LOC) → `revops-analytics` (4,599 LOC:
classification, demand_flow, flow, growth, kalman, policy, profitability,
protection, telemetry) — all with test files. `[x]` as kernels, `[~]` as wired
surfaces: Task 49 registered `revenue-profitability` / `revenue-analyze` as
**reachable** RPCs, but neither has a live evidence-assembly pipeline behind
it yet (no `ChannelProfitability`/`FlowMetrics` fetch from
`listpeerchannels`/forward-history) — both now return an explicit in-band
`not_yet_ported` marker (Task 50's F3/F4/F5 fix; round-2 correction, P1:
this used to say "honest no-data/`_gaps` shape", which described the
pre-Task-50 shape that collided with Python's own legitimate answers — see
the historical appendix under §3's Task 49 section), not real per-channel
analysis and not a shape Python could ever legitimately return itself.

---

## 3b. What "full functional parity" actually requires

Ranked by size, from this audit:

1. **Capital allocation (~10,400 Python LOC)** — capacity planner, capex
   budget, Boltz manager + auto-cycle, LN+ swap automation. (Task 52 refresh:
   "Nothing exists" is stale — substantial pure kernels, planning
   orchestration, the Boltz subprocess transport, and the LN+ wiring layer
   now exist per Lens 4 above. What does NOT exist is any caller, concrete
   wire transport, execution call site, or registered RPC — so this remains
   the item where real sats are furthest from being safely governable.)
2. **Operator RPC surface (50 of 69 Python-equivalent methods remaining,
   post-Task-49)** — everything except the 19 now registered (status/history/
   report/dashboard/fee-debug/fee-wake plus Task 49's ten Batch A methods).
   Round-2 correction, P1: this was 60 of 69 before Task 49 registered its
   ten Python-equivalent builders (9 → 19 registered, so 69 − 19 = 50
   remaining, not 60).
   **Task 66 correction (2026-07-31, `c4ba670`): 0 of 69 remaining — the
   registration surface is CLOSED (see the Task 66 subsection in §2).
   Registration ≠ done: 54 of 69 are `partial` (complete contracts with
   sealed execution, or declared response deltas) and every Task-66
   contract awaits independent review, but "which Python methods have no
   Rust registration at all" is no longer a live category.**
3. **Wiring the ported-but-uncalled kernels** — rebalance (8,167 LOC ported, no
   loop, no RPC, no sendpay call site) and the non-fee governed-execution call
   sites. Same shape as A1–A3, at much larger scale and touching real payments.
4. **Retention/pruning** — forwards pruning and the 8 runway tables.
5. **Remaining fee-path items** — the A3 LIVE-broadcast half (shadow half
   closed by Task 44 at `accee49`; see §2 A3) and `note_fee_applied` — both
   land with the whole-plugin live scheduler, not before.

**Round-2 correction (P1): the paragraph below is HISTORICAL, describing the
fee-only cutover boundary as it stood before the 2026-07-27 operator-approved
whole-plugin-replacement decision (see the "Scope note" at the top of this
document).** It is kept for the historical record of what was true when it
was written, not as the current programme boundary — do not read "does not
block the fee cutover" as "does not block Rust replacing the plugin", which
is the now-approved direction:

> ~~None of 1–4 blocks the fee cutover~~, which was scoped to the fee
> subsystem and left Python owning everything else. They blocked "Rust
> replaces the plugin". Those were different programmes at the time and were
> not to be conflated.

**Current conclusion:** under the whole-plugin-replacement scope, items 1-4
above ARE the remaining program — there is no longer a smaller "fee cutover
only" scope that lets them stay non-blocking. Capital allocation, the
50-method remaining RPC surface, the ported-but-uncalled kernels, and
retention/pruning are all in scope for "Rust replaces the plugin" and must
each be planned, ported, and independently reviewed before that cutover, per
`docs/superpowers/specs/2026-07-27-whole-plugin-rust-cutover-design.md`.

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
