# Phase 4B — Fee-Controller Wiring + Dry-Run Window Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the fully-ported, fixture-proven Phase 4 fee controller (`revops-fees`) into the live `revops` plugin binary and run a journaled dry-run window on lnnode alongside the authoritative Python plugin, with zero writes to anything Python reads.

**Architecture:** A single-owner fee-cycle thread inside `crates/revops` owns `ControllerState` + one long-lived `PyRandom`; each cycle it re-hydrates per-channel state from the production DB (read-only), consumes a per-cycle-frozen `EvidenceSnapshot` (async-prefetched RPC + sync read-only DB), runs `run_fee_cycle`, and appends `FeeDecision`s to the JSONL dry-run journal. No `setchannel`/`htlcmax` broadcast exists anywhere in this phase — the broadcast path is a cutover deliverable (checklist item 9), so dry-run safety is structural, not flag-guarded.

**Tech Stack:** Rust 1.97 (`rust-toolchain.toml`), `cln-plugin` 0.7.0, `rusqlite` (system-linked, never `bundled`), tokio, existing crates `revops-fees` / `revops-econ` / `revops-db` / `revops-rpc`; `tools/diff-harness/diff_fee_decisions.py` as the gate instrument.

## Input (verbatim, from `.superpowers/sdd/progress.md` Phase 4 final review)

> PHASE 4B WIRING CHECKLIST (verbatim from final review, the next plan's input): (1) config plumbing all 22 FeeCfgSnapshot fields incl DB-override-only keys + neighbor_median_min_competitors plumb-or-verify==3; (2) FeeEvidence impl over read-only actor + rpc snapshots (channel_states row order, gossip via FrozenObservations/GossipCache, chain_costs FALSINESS mapping documented, mempool_ma_24h depends on Python still recording, clear_exploration_flag strict no-op); (3) state lifecycle DECISION: re-hydrate-per-cycle vs seed-once (decide explicitly; StateSink never production DB); (4) journal db_dir wiring + harness --journal alignment; (5) scheduler: single-owner cycle task, ONE long-lived PyRandom seeded at start, clock once per cycle; (6) governor plumbing (EconLedger + registry into GovernedDeps); (7) RPC surface revenue-r-fee-debug + wake/policy notification triggers; (8) live window: diff_fee_decisions over hours, zero deterministic mismatches = gate, add --until first; (9) DEFERRED TO CUTOVER: setchannel/htlcmax broadcast behind flag, forward_event hook, set_initial_fee. PRE-DRY-RUN VERIFICATIONS: journal path explicit; lnnode config neighbor_median_min_competitors check; chain-costs falsiness mapping; fee_changes strict-parse watch items.

Checklist→task map: (1)→T1, (2)→T2, (3)→Design Note 1 + T4, (4)→T3, (5)→T6, (6)→T5, (7)→T7, (8)→T3 (`--until`) + T8 (window), (9)→"Deferred to Cutover" section below. PRE-DRY-RUN VERIFICATIONS→T8 Steps 1–4.

## Global Constraints

- **Python stays authoritative for the whole window.** The Rust plugin never writes the production DB (`revops-r-db-path` is `revops_db::actor::spawn_read_only` only), never calls `setchannel`, never touches `econ_ledger.db`. Any new write target must be a Rust-owned file next to `revops-r-observer.db`.
- **No broadcast code in this phase at all** — `setchannel`/`htlcmax` broadcast, `forward_event` fee-nudge hook, and `set_initial_fee` are cutover work (checklist item 9). A reviewer finding any `setchannel` call in this phase's diff rejects the task.
- **New option `revops-r-fee-dryrun` (bool, default `false`, dynamic).** The fee-cycle scheduler does not start unless it resolves `true`. Default OFF so a deploy/restart without explicit opt-in changes nothing.
- **ONE long-lived `PyRandom` seeded once at scheduler start; clock (`now_unix()`) read exactly once per cycle** and threaded through `CycleDeps::now` (checklist item 5).
- **`FeeEvidence` is per-cycle frozen** — the impl must not issue RPC or observe new DB rows after the cycle starts; `clear_exploration_flag` is a strict no-op over this read-only surface (T10 review contract item 2).
- **Release-profile float parity gate stays closed:** the deployed binary is `--release`; `cargo test --workspace --release` (CI leg from the float-hardening pass) must be green before any binary ships. New float call sites in fixture-compared paths use `revops_econ::pyfloat::py_pow`, never bare `.powf`.
- **Diff-harness skip discrimination** stays `would_broadcast`/`algorithm_values: null` — never `reason_code` alone (T10 review contract item 1).
- **rusqlite is system-linked** (workspace constraint); `ldd` compat check against lnnode before every ship, per `docs/runbooks/observer-deploy.md` §1.
- **Gates per task:** `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` (dev) — plus the release leg before deploy. Worktree isolation per the established Phase 2–4 pattern; the controller commits merges.

---

## Design Note 1 (REQUIRED FIRST): state lifecycle — **re-hydrate-per-cycle** (recommendation)

**Decision needed (checklist item 3):** does the Rust controller seed `ControllerState` once at start and evolve it in memory (Python's own production behavior), or re-hydrate it from the production DB's `v2_state_json` blobs at the top of every cycle?

**Recommendation: re-hydrate-per-cycle for the whole dry-run window; flip to seed-once at cutover.**

Reasoning:

1. **The window's gate is "zero deterministic mismatches" per (channel, cycle-window) pair** (`diff_fee_decisions.py`, 120s tolerance). Python broadcasts its decisions — its fees actually change, its PID integrators, Thompson posteriors, cooldown/`last_broadcast_at` stamps and Vegas state advance on the *broadcast* trajectory. A seed-once Rust controller in dry-run never broadcasts, so from cycle 2 onward its state inputs diverge *by design* (not by bug): every stateful component would drift, mismatches would accumulate unboundedly, and the gate would be unreachable while telling us nothing about porting fidelity.
2. **Re-hydration turns every cycle into an independent parity trial:** both controllers start the cycle from the *same* persisted state (Python's flush from its previous cycle), see the same evidence, and must emit the same decisions. That is exactly the property the diff instrument measures.
3. **The hydration path is already proven byte-exact:** Phase 4 T9's production gate replayed 40/40 lnnode `v2_state_json` blobs round-trip byte-identically, and `state_store::{read_fee_strategy_rows, parse_v2_blob, load_fee_state, load_cycle_state}` are the exact functions this plan calls. Seed-once exercises nothing T9 hasn't already proven; re-hydrate exercises it against live data every cycle.
4. **Cost/risk:** re-hydrate adds one read-only query per cycle (`read_fee_strategy_rows` over ~40 rows — negligible) and one timing hazard: Rust must hydrate *after* Python's end-of-cycle state flush, which the scheduler handles by offsetting its tick (T6 Step 4). Seed-once's only advantage — exercising the in-memory evolution loop — is precisely the configuration cutover will run, so it is validated at cutover rehearsal, not during a window whose purpose is decision parity.
5. **`StateSink` never points at the production DB** in either mode (checklist item 3's hard rule). During the window the sink is `JournalStateSink` (T4): it serializes what *would* be flushed into the dry-run journal directory for offline inspection, writing only Rust-owned files.

**Consequence for cutover (recorded now):** at cutover, hydrate ONCE from the production DB at start (Python's final flush is the seed), then run seed-once with `StateSink` pointing at the Rust-owned writable DB. That flip is a one-line scheduler config change (T6's `StateLifecycle` enum), not a rework.

### ADDENDUM (Phase 4b Task 8b, 2026-07-17): the skip gate needs its OWN cross-cycle memory — re-hydrate-per-cycle is NOT enough for it

**What was wrong.** Point 2 above claimed both controllers "start the cycle from the *same* persisted state (Python's flush from its *previous* cycle)." **That intent was never achievable and was never implemented.** The dry-run window produced 240/240 skip rows (100% `skip_waiting_time`/`skip_sleeping`, 0 broadcasts) against 27 real Python decisions — every cycle skipped every channel. Root cause (full evidence: `.superpowers/sdd/fee-window-diagnosis.md`, H1, design/timing — NOT a porting bug; the gate in `revops-fees/cycle.rs` is byte-identical to `fee_controller.py`):

- The FlushTriggered scheduler can only wake `rehydrate()` **after** it observes Python's `fee_strategy_state` flush marker advance. **That flush-marker advance IS the current cycle completing.** By the time any signal is observable, Python has already overwritten `last_update` to `now` for the very cycle Rust is trying to reproduce. There is no DB state "after Python's previous flush, before Python's current decision" for Rust to catch — it does not exist as an observable.
- So the freshly re-hydrated `cycle.last_update` Rust feeds the skip gate is Python's *current-cycle* output (proven on-node: 35s fresh), giving `pre_hours_elapsed ≈ 0.0097h` against a `min_observation_hours` floor of `0.25`/`1.0h` → guaranteed skip. Python's identical gate passes because Python reads `pre_last_update` **before** writing the new flush — i.e. the PREVIOUS cycle's value.

**The fix (implemented).** Re-hydrate-per-cycle stays correct for everything that should track Python's broadcast trajectory (fee posteriors, PID, Vegas, cooldown/`last_broadcast_at`, forward-count-since, gossip-refresh, force-reprice). **Only the skip gate's two pre-decision inputs** (`pre_last_update` and the per-channel `pre_is_sleeping` determinant) must come from Rust's **own cross-cycle memory**, not from the freshly-flushed blob:

- `ControllerState` gains `skip_gate_prev`/`skip_gate_seen` (`SkipGateEpoch { last_update, is_sleeping }` per channel). `revops::fee_state::rehydrate` records THIS cycle's fresh hydration as `skip_gate_seen` and promotes the prior cycle's `seen` into `skip_gate_prev`. The gate (`revops-fees/cycle.rs::process_channel`) reads `skip_gate_prev` — **the value Rust's own previous triggered cycle hydrated, which IS the pre-decision epoch Python's current-cycle decision was conditioned on** (Rust's previous cycle reproduced Python's previous cycle, hydrating after Python's previous flush = Python's current pre-decision value). Everything else keeps consuming the freshly rehydrated live state.
- **BOOTSTRAP:** Rust's first triggered cycle after (re)start (and any channel's first appearance in the DB) has no cached prior. Rust *cannot* know Python's pre-decision epoch for a cycle whose predecessor flush it never observed, so that channel-cycle is **explicitly non-comparable**: the gate falls back to the live value AND the decision trace is flagged `skip_gate_comparable: false`. `tools/diff-harness/diff_fee_decisions.py` excludes such Rust lines and any same-window Python `fee_changes` row from the pass/fail contract (INFO, not "rust missed a decision") — one cycle per channel per restart, honestly marked, never a spurious miss. This matches Python's cold-start semantics closely enough (a `pre_last_update` Rust cannot trust is treated as un-actionable rather than fabricated) while keeping the window's decision-parity signal clean.
- **T6b COALESCING (window watch item, documented not hidden):** if two Python flushes merge into one Rust cycle inside the settle window, `skip_gate_prev` holds Rust's OWN last-observed hydration, which is then **one Python-cycle stale** — it represents the pre-(N-1) epoch, while Python's cycle-N decision was conditioned on the post-(N-1) epoch. This biases `pre_hours_elapsed` *upward* (older cached `last_update` → more elapsed → more permissive → toward evaluate, the OPPOSITE of the original all-skip failure). It is bounded and, in practice, near-impossible from Python's main loop (decision cadence is hours — `dts_pid_sample` gaps of 4–8h on-node — vs a 30s settle window); the realistic coalescing source (out-of-cycle prune/hook/manual marker steps) does not rewrite surviving channels' `last_update`, so their cached epoch stays valid. Pinned by `rehydrate_coalescing_caches_own_prior_observation_not_missed_flush`. **Watch item:** if a live window ever shows a full second decision cycle coalesced into one Rust cycle for the same channel within the settle window, exclude that channel-cycle manually — the gate cannot self-detect it without plumbing the FlushWatcher's coalesce count into the cycle.
- **"now"-skew confirmed within tolerance:** once `pre_last_update` is the correct (hours-old) epoch, Rust's cycle `now` running ~30–60s after Python's decision `now` shifts `pre_hours_elapsed` by only ≈0.008–0.017h — negligible against the 0.25/1.0h floor except in a razor-thin boundary band. The diagnosis's finding holds: the fresh-`last_update` epoch error (0.0097h vs 0.25h) was the dominant fault, not the now-skew.

**SeedOnce (cutover) interaction:** the cross-cycle cache is populated only by per-cycle `rehydrate`. In `SeedOnce` the single seed hydration leaves `skip_gate_prev` empty from cycle 2 on, so the gate falls back to the live in-memory state — **byte-identical gate decisions to the pre-T8b code** (the `skip_gate_comparable: false` trace key is emitted but inert; no diff harness runs at cutover). Design Note 1's core recommendation (re-hydrate-per-cycle to avoid unbounded posterior/PID drift) stands unchanged; only the skip gate is carved out onto independent Rust-side memory.

---

## File Structure

- `crates/revops/src/fee_config.rs` — NEW: `FeeCfgSnapshot` resolver (T1)
- `crates/revops/src/fee_evidence.rs` — NEW: `EvidenceSnapshot` + `FeeEvidence` impl (T2)
- `crates/revops/src/fee_state.rs` — NEW: hydration + `JournalStateSink` (T4)
- `crates/revops/src/fee_governor.rs` — NEW: `GovernorWiring` (T5)
- `crates/revops/src/fee_scheduler.rs` — NEW: single-owner cycle thread + `CycleMsg` (T6)
- `crates/revops/src/main.rs` — MODIFY: options (`fee-dryrun`, `journal-dir`), scheduler spawn, `revenue-r-fee-debug` RPC, wake triggers (T3/T6/T7)
- `crates/revops/src/lib.rs` — MODIFY: `pub mod` lines for each new module (folded into each task)
- `tools/diff-harness/diff_fee_decisions.py` — MODIFY: `--until`, `DEFAULT_JOURNAL_PATH` (T3)
- `docs/runbooks/fee-dryrun-deploy.md` — NEW: deploy/rollback runbook (T8)
- Tests: `crates/revops/tests/fee_config.rs`, `crates/revops/tests/fee_evidence.rs`, `crates/revops/tests/fee_state.rs`, `crates/revops/tests/fee_scheduler.rs`

## Wave Table

| Wave | Tasks | Parallel-safe? | Why |
|---|---|---|---|
| 0 | T1 config resolver, T2 evidence impl, T3 journal dir + harness `--until` | YES (3 worktrees) | disjoint files; T1/T2 share only read access to `revops-fees` types |
| 1 | T4 state lifecycle, T5 governor wiring | YES (2 worktrees) | disjoint files; both consume Wave-0 merges |
| 2 | T6 scheduler + dry-run option | NO (serial) | consumes T1–T5 interfaces; owns `main.rs` wiring |
| 3 | T7 fee-debug RPC + wake/policy triggers | NO (serial) | extends T6's `CycleMsg` seam |
| 4 | T8 pre-dry-run verifications + deploy + live window | CONTROLLER-RUN | touches lnnode; human-gated |

---

### Task 1: `FeeCfgSnapshot` resolver (checklist item 1)

**Files:**
- Create: `crates/revops/src/fee_config.rs`
- Modify: `crates/revops/src/lib.rs` (add `pub mod fee_config;`)
- Test: `crates/revops/tests/fee_config.rs`

**Interfaces:**
- Consumes: `revops_fees::cycle::FeeCfgSnapshot` (22 fields, `Default` == Python `Config` defaults, drift-guard-tested); `revops::config_resolve::{db_override_key, validate_override, python_option_name, is_immutable_key}`; `revops_db::queries::config_override`; the init-cached `python_option_values: HashMap<String, cln_plugin::options::Value>` from `State`.
- Produces: `pub async fn resolve_fee_cfg(db: Option<&revops_db::actor::DbHandle>, python_option_values: &HashMap<String, cln_plugin::options::Value>) -> FeeCfgSnapshot` and `pub fn neighbor_median_min_competitors_ok(resolved: &serde_json::Value) -> bool` (returns `resolved == 3`). T6's scheduler calls `resolve_fee_cfg` at the top of EVERY cycle (per-cycle resolution is what makes runtime `revenue-config set` changes on the Python side visible — Python's runtime mutations land in the `config_overrides` table, which layer (a) re-reads).

Resolution order per field (same precedence `revenue-r-config` already implements): (a) DB override via `queries::config_override` + `validate_override` → (b) cached `listconfigs` Python option value → (c) `FeeCfgSnapshot::default()` field. **DB-override-only keys** (`paused`, `authority_level`, `econ_governor_fees_enabled` — no CLN option exists, ledger note "17 PUBLIC_RUNTIME_KEYS") skip layer (b) entirely: (a) → (c).

`neighbor_median_min_competitors` is **verify==3, not plumb** (the market functions bake `market::MIN_COMPETITORS = 3`; `FeeCfgSnapshot` is a frozen 22-field contract with drift-guard tests — do not add a field). The scheduler (T6) resolves the key each cycle and **fails closed**: if it resolves ≠ 3, log `revops: fee cycle disabled: neighbor_median_min_competitors={v} != baked 3` and skip the cycle. T8 additionally verifies the live lnnode value before the window opens.

- [ ] **Step 1: Write the failing test** (`crates/revops/tests/fee_config.rs`):

```rust
use revops_fees::cycle::FeeCfgSnapshot;

#[tokio::test]
async fn resolve_fee_cfg_defaults_when_no_db_no_python() {
    let cfg = revops::fee_config::resolve_fee_cfg(None, &std::collections::HashMap::new()).await;
    assert_eq!(cfg, FeeCfgSnapshot::default());
}

#[tokio::test]
async fn resolve_fee_cfg_db_override_beats_listconfigs() {
    // fixture db with config_overrides row: max_fee_ppm = "1500"
    let (handle, _tmp) = fixture_db_with_override("max_fee_ppm", "1500").await;
    let mut py = std::collections::HashMap::new();
    py.insert("revenue-ops-max-fee-ppm".to_string(),
              cln_plugin::options::Value::Integer(1234));
    let cfg = revops::fee_config::resolve_fee_cfg(Some(&handle), &py).await;
    assert_eq!(cfg.max_fee_ppm, 1500);
}

#[tokio::test]
async fn resolve_fee_cfg_db_override_only_keys_skip_listconfigs() {
    // paused has NO CLN option: a (fake) listconfigs value must be ignored.
    let (handle, _tmp) = fixture_db_with_override("paused", "true").await;
    let cfg = revops::fee_config::resolve_fee_cfg(Some(&handle), &std::collections::HashMap::new()).await;
    assert!(cfg.paused);
}
```

Plus one test per remaining field class (int/float/string/bool/`enable_dynamic_htlcmax` raw-Value passthrough, `authority_level` `Some("capital")` default) — a 22-row table test walking every field name against an override fixture, asserting no field is silently unplumbed:

```rust
#[tokio::test]
async fn all_22_fields_are_plumbed() { /* table of (field, override_raw, expected) covering every FeeCfgSnapshot field */ }
```

- [ ] **Step 2: Run to verify failure.** `cargo test -p revops --test fee_config` → FAIL: `fee_config` module not found.
- [ ] **Step 3: Implement `fee_config.rs`.** One `resolve_*` helper per type; `enable_dynamic_htlcmax` keeps the RAW resolved value as `serde_json::Value` (admission's narrow truthiness — do not coerce to bool). Reuse `config_resolve`'s existing precedence helpers; do not duplicate validation.
- [ ] **Step 4: Run to verify pass.** `cargo test -p revops --test fee_config` → PASS; `cargo clippy -p revops --all-targets -- -D warnings` clean.
- [ ] **Step 5: Commit.** `git add -A && git commit -m "feat(wiring): per-cycle FeeCfgSnapshot resolver, all 22 fields incl DB-override-only keys"`

---

### Task 2: `EvidenceSnapshot` — live `FeeEvidence` over read-only DB + RPC snapshots (checklist item 2)

**Files:**
- Create: `crates/revops/src/fee_evidence.rs`
- Modify: `crates/revops/src/lib.rs`
- Test: `crates/revops/tests/fee_evidence.rs`

**Interfaces:**
- Consumes: `revops_db::open_read_only(path) -> Result<Connection>` (a per-cycle direct read-only `Connection`, NOT the async actor — `FeeEvidence` is a sync trait and the snapshot lives on the cycle thread); `revops-rpc` snapshot fetchers (`listpeerchannels`, `listchannels`, plus whatever `hydration`/`config_resolve` already use for socket RPC); `revops_fees::cycle::{FeeEvidence, ChannelInfo, ChannelStateRow, GossipRow, PeerFeeHistory}`; `revops_fees::market` `FrozenObservations`/`GossipCache`; `revops_fees::drain::NodeChannel`.
- Produces:

```rust
pub struct EvidenceSnapshot { /* owned prefetched RPC data + read-only Connection + RefCell gossip memo */ }

/// Async half: fetch ALL RPC snapshots (listpeerchannels, listchannels once,
/// getinfo node id) BEFORE the cycle starts. Returns everything owned.
pub async fn prefetch_rpc(socket_path: &std::path::Path) -> anyhow::Result<RpcPrefetch>;

/// Sync half, called ON the cycle thread: opens the read-only Connection.
pub fn build_evidence_snapshot(db_path: &std::path::Path, rpc: RpcPrefetch, now: i64)
    -> anyhow::Result<EvidenceSnapshot>;

impl revops_fees::cycle::FeeEvidence for EvidenceSnapshot { /* all 19 methods incl. the 3 defaulted ones */ }
```

Contract points (each gets its own test):
- **`channel_states()` row order:** copy the SQL text of `get_all_channel_states` from `/home/sat/bin/cl_revenue_ops-port/modules/database.py` VERBATIM (including its ORDER BY / natural rowid order) and preserve result order into the `Vec<ChannelStateRow>`.
- **`gossip_channels(peer_id)`:** one `listchannels` prefetch per cycle, grouped by destination through the existing `FrozenObservations`/`GossipCache` memo pattern — never a per-peer RPC.
- **`chain_costs()` FALSINESS mapping — documented in a doc comment on the method impl:** Python's use site treats a missing/empty/falsy chain-costs dict as "no chain costs"; the impl maps the DB row → `None` exactly when Python's `if not chain_costs:` branch would fire (empty dict, NULL row, or all-zero — cite the exact Python line in the doc comment when implementing).
- **`mempool_ma_24h()`:** SQL port of `database.get_mempool_ma(86400)`. Doc comment MUST state: value is only live because *Python* keeps recording mempool samples during the window; after Python unloads (cutover) this returns stale data until the Rust recorder exists — a cutover rider.
- **`clear_exploration_flag()` strict no-op** with a doc note citing the T10 review adjudication; test asserts calling it changes nothing observable on the snapshot.

- [ ] **Step 1: Write failing tests** against a fixture DB (reuse `fixtures/schema.sql` + seeded rows) and a canned `RpcPrefetch`:

```rust
#[test]
fn channel_states_preserves_python_row_order() { /* seed 3 rows out of id order; assert Vec order matches Python's query order */ }
#[test]
fn gossip_channels_memoizes_single_listchannels_prefetch() { /* two calls, same peer -> same Vec, zero extra fetches (RpcPrefetch consumed once by construction) */ }
#[test]
fn chain_costs_falsiness_maps_to_none() { /* empty/NULL/all-zero row -> None; populated row -> Some */ }
#[test]
fn clear_exploration_flag_is_strict_noop() { /* exploration_flag() identical before/after */ }
#[test]
fn mempool_ma_24h_matches_python_sql() { /* seeded samples -> expected mean */ }
```

- [ ] **Step 2:** `cargo test -p revops --test fee_evidence` → FAIL (module missing).
- [ ] **Step 3: Implement.** All `FeeEvidence` methods are sync reads over the owned `Connection`/prefetched structs. No method may call RPC.
- [ ] **Step 4:** `cargo test -p revops --test fee_evidence` → PASS; clippy clean.
- [ ] **Step 5: Commit.** `git commit -m "feat(wiring): EvidenceSnapshot — per-cycle-frozen FeeEvidence over read-only DB + prefetched RPC"`

---

### Task 3: Journal dir wiring + harness `--journal`/`--until` alignment (checklist items 4, 8-prep)

**Files:**
- Modify: `crates/revops/src/main.rs` (new option), `tools/diff-harness/diff_fee_decisions.py`
- Test: `crates/revops/tests/fee_config.rs` (option presence), harness `--self-test`

**Interfaces:**
- Consumes: `revops_fees::journal::Journal::{open_dir, at_path}`; `main.rs`'s option-registration pattern (`DefaultStringConfigOption`, `opt_name`, `config_name_map`).
- Produces: option `revops-r-journal-dir` (string, default `""`). Empty default resolves at scheduler start to **the parent directory of the resolved `observer-db-path`** (a Rust-owned writable location by construction); a non-empty value is used as-is after `expand_tilde`. `pub fn resolve_journal_dir(journal_dir_opt: &str, observer_db_path: Option<&std::path::Path>) -> Option<std::path::PathBuf>` in `fee_scheduler.rs`'s module (placed there by T6; this task lands it in `main.rs` as a free fn and T6 moves nothing — keep it in `main.rs`).

Harness changes:
1. `DEFAULT_JOURNAL_PATH` becomes `/data/lightningd/.lightning/fee_dryrun_journal.jsonl` (the observer-db directory on lnnode + `journal::JOURNAL_FILE_NAME`) — the T11 ledger note said the old `~/.lightning/...` default was inferred pre-wiring; this task closes that.
2. Add `--until <unix_ts>` (mirror of `--since`): rows/decisions with timestamp `> until` are excluded from matching on BOTH sides, so a live window can be bounded and re-run reproducibly. Checklist item 8 says "add `--until` first" — this task is Wave 0 for exactly that reason.
3. Extend the harness self-test with one scenario proving `--until` excludes a later decision (self-test count goes 10 → 11).

- [ ] **Step 1 (failing):** run `python3 tools/diff-harness/diff_fee_decisions.py --self-test` after adding self-test scenario 11 (an out-of-window pair that must be excluded under `--until`) — FAILS because `--until` doesn't exist.
- [ ] **Step 2: Implement `--until`** in `main(argv=...)` next to `--since`; apply symmetrically in the journal reader and the `fee_changes` SQL (`AND timestamp <= :until`).
- [ ] **Step 3:** `python3 tools/diff-harness/diff_fee_decisions.py --self-test` → `ALL PASS` (11 scenarios). `python3 -m py_compile tools/diff-harness/diff_fee_decisions.py` → OK.
- [ ] **Step 4: Register `revops-r-journal-dir`** in `main.rs` (same block as `observer-db-path`; add to `config_name_map`), plus unit test `config_name_map_includes_journal_dir` mirroring the existing `observer-db-path` test.
- [ ] **Step 5:** `cargo test -p revops` → PASS.
- [ ] **Step 6: Commit.** `git commit -m "feat(wiring): revops-r-journal-dir option; harness --until + lnnode journal default"`

---

### Task 4: State lifecycle — per-cycle hydration + `JournalStateSink` (checklist item 3)

**Files:**
- Create: `crates/revops/src/fee_state.rs`
- Modify: `crates/revops/src/lib.rs`
- Test: `crates/revops/tests/fee_state.rs`

**Interfaces:**
- Consumes: `revops_fees::state_store::{read_fee_strategy_rows, parse_v2_blob, load_fee_state, load_cycle_state, FeeStrategyRow}`; `revops_fees::cycle::{ControllerState, StateSink, ChannelCycleState, ChannelFeeState, serialize_cycle_state_payload}`; `revops_fees::state_store::fee_state_to_v2_dict`; `revops_econ` `dumps_python` (whatever `journal.rs` uses for JSONL rendering — reuse it).
- Produces:

```rust
/// Design Note 1: called at the top of EVERY dry-run cycle. Vegas state and
/// vegas_wake_armed are process-lifetime (Python keeps them as module globals,
/// not in v2_state_json) — hydration REPLACES cycle_states/fee_states and
/// PRESERVES state.vegas / state.vegas_wake_armed / last_decision_summary.
pub fn rehydrate(state: &mut ControllerState, conn: &rusqlite::Connection);

/// StateSink that never touches the production DB: serializes each flushed
/// row as one JSONL line {"channel_id":..., "v2_state_json":...} into
/// <journal_dir>/fee_dryrun_state.jsonl for offline comparison.
pub struct JournalStateSink { /* path */ }
impl JournalStateSink { pub fn open_dir(dir: &std::path::Path) -> std::io::Result<Self>; }
impl revops_fees::cycle::StateSink for JournalStateSink {
    fn flush_batch(&self, rows: &[(String, ChannelCycleState, ChannelFeeState)]);
}
```

- [ ] **Step 1: Failing tests:**

```rust
#[test]
fn rehydrate_replaces_channel_maps_and_preserves_vegas() { /* seed fixture db with 2 v2 blobs; pre-poison state.vegas + insert a stale channel; assert maps rebuilt, vegas untouched, stale channel gone */ }
#[test]
fn rehydrate_round_trips_t9_fixture_blob_byte_identically() { /* load one committed T9 production blob fixture, rehydrate, serialize back via fee_state_to_v2_dict/serialize_cycle_state_payload, compare bytes */ }
#[test]
fn journal_state_sink_writes_one_line_per_row_and_never_opens_production_db() { /* flush 2 rows -> 2 JSONL lines in tmpdir */ }
```

- [ ] **Step 2:** `cargo test -p revops --test fee_state` → FAIL.
- [ ] **Step 3: Implement** (`rehydrate` = `read_fee_strategy_rows` → per row `parse_v2_blob` → `load_fee_state`/`load_cycle_state` → insert into fresh `BTreeMap`s, then swap).
- [ ] **Step 4:** tests PASS; clippy clean.
- [ ] **Step 5: Commit.** `git commit -m "feat(wiring): re-hydrate-per-cycle state lifecycle + JournalStateSink (Design Note 1)"`

---

### Task 5: Governor plumbing (checklist item 6)

**Files:**
- Create: `crates/revops/src/fee_governor.rs`
- Modify: `crates/revops/src/lib.rs`
- Test: unit tests inside `fee_governor.rs`

**Interfaces:**
- Consumes: `revops_econ::ledger::EconLedger::open(path)`; `revops_econ::arbiter::ActiveIntentRegistry::new(None)`; `revops_fees::execution::GovernedDeps`.
- Produces:

```rust
/// Owns the Rust-side governor objects for the plugin lifetime. During the
/// dry-run window the ledger is the Rust plugin's OWN file
/// (<journal_dir>/econ_ledger_dryrun.db) — NEVER production econ_ledger.db:
/// governed authorization APPENDS intent_proposed events, and Python owns the
/// production ledger until cutover.
pub struct GovernorWiring {
    ledger: Option<EconLedger>,
    registry: ActiveIntentRegistry,
}
impl GovernorWiring {
    pub fn open(journal_dir: Option<&std::path::Path>) -> Self; // ledger None if dir absent or open fails (log once)
    /// Borrow as GovernedDeps for one cycle. paused/authority_level come from
    /// the CURRENT cycle's FeeCfgSnapshot (T1) — not cached at start.
    pub fn governed_deps<'a>(&'a self, cfg: &revops_fees::cycle::FeeCfgSnapshot) -> GovernedDeps<'a>;
}
```

`CycleDeps.governed` is `Some(...)` only when `cfg.econ_governor_fees_enabled` (the cycle consults it only then anyway — pass it unconditionally and let the cycle gate, matching `CycleDeps`'s doc).

- [ ] **Step 1: Failing tests:** `governed_deps_mirrors_cfg_paused_and_authority`, `open_without_dir_yields_none_ledger_and_still_constructs`, `ledger_path_is_dryrun_file_not_production` (assert the opened path ends with `econ_ledger_dryrun.db`).
- [ ] **Step 2:** `cargo test -p revops fee_governor` → FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** PASS; clippy clean.
- [ ] **Step 5: Commit.** `git commit -m "feat(wiring): GovernorWiring — dry-run EconLedger + ActiveIntentRegistry into GovernedDeps"`

---

### Task 6: Single-owner fee-cycle scheduler + `revops-r-fee-dryrun` option (checklist item 5)

**Files:**
- Create: `crates/revops/src/fee_scheduler.rs`
- Modify: `crates/revops/src/main.rs` (register option; spawn scheduler after `configured.start(state)`; plumb socket path, db path, journal dir)
- Modify: `crates/revops/src/lib.rs`
- Test: `crates/revops/tests/fee_scheduler.rs`

**Interfaces:**
- Consumes: everything from T1–T5; `revops_fees::cycle::{run_fee_cycle, ControllerState, CycleDeps}`; `revops_fees::pyrand::PyRandom`; `revops_fees::journal::Journal`.
- Produces:

```rust
pub enum CycleMsg {
    /// T7 wake triggers and debug queries extend this enum.
    RunCycleNow,
    Query(FeeDebugQuery, std::sync::mpsc::Sender<serde_json::Value>),
    Shutdown,
}

pub struct SchedulerConfig {
    pub db_path: std::path::PathBuf,          // production, read-only
    pub socket_path: std::path::PathBuf,      // lightning-rpc
    pub journal_dir: std::path::PathBuf,      // T3 resolution
    pub lifecycle: StateLifecycle,            // RehydratePerCycle (window) | SeedOnce (cutover)
}
pub enum StateLifecycle { RehydratePerCycle, SeedOnce }

/// Spawns: (a) one dedicated std::thread OWNING ControllerState + the ONE
/// long-lived PyRandom (seeded from now_unix() at spawn, once); (b) one tokio
/// ticker task that each fee_interval does the ASYNC prefetch (T1 resolve_fee_cfg
/// + T2 prefetch_rpc) and sends the prepared inputs over the channel.
/// Returns a cheap handle for T7's RPC/wake senders.
pub fn spawn(cfg: SchedulerConfig, db_handle: Option<revops_db::actor::DbHandle>,
             python_option_values: HashMap<String, cln_plugin::options::Value>)
    -> SchedulerHandle;
pub struct SchedulerHandle { pub tx: std::sync::mpsc::Sender<CycleMsg>, /* + tokio side sender */ }
```

Per-cycle sequence on the owner thread (each numbered point is asserted by a test):
1. `let now = now_unix();` — **exactly once**; every downstream consumer gets this value.
2. Receive prefetched `(FeeCfgSnapshot, RpcPrefetch)` from the async side; skip cycle (log) if `neighbor_median_min_competitors_ok` is false (T1 fail-closed rule).
3. `build_evidence_snapshot(db_path, rpc, now)`; on error log + skip cycle (never panic — the hub precedent: closures must be non-panicking).
4. If `RehydratePerCycle`: `rehydrate(&mut state, snapshot.conn())`. Tick alignment: the ticker fires at `fee_interval` with a fixed `+120s` phase offset from plugin start so Rust's hydrate lands after Python's end-of-cycle flush (the same 120s tolerance the diff harness matches with).
5. Build `CycleDeps { evidence, cfg, rng: &mut the_one_pyrandom, now, governed, journal: Some(&journal), state_sink: Some(&journal_state_sink) }`.
6. `let decisions = run_fee_cycle(&mut state, &mut deps);`
7. `journal.append_all(&decisions)` — journal write failures log loudly (a silent journal gap invalidates the window) but never crash the plugin.

`main.rs` wiring: register `revops-r-fee-dryrun` (`DefaultBooleanConfigOption`, default `false`, `.dynamic()`); after `configured.start(state)`, spawn the scheduler ONLY IF the option resolved `true` AND `db_path`/`observer_db`-derived journal dir are available — otherwise log exactly why the fee cycle is off. Store `Option<SchedulerHandle>` in `State` (Arc'd) for T7.

- [ ] **Step 1: Failing tests** (script the seams — fake evidence via a `MockEvidence` already used by `revops-fees` tests, temp journal dir):

```rust
#[test] fn scheduler_uses_one_clock_read_per_cycle() { /* CountingClock injected via #[cfg(test)] seam; 1 cycle -> 1 read */ }
#[test] fn scheduler_seeds_pyrandom_exactly_once_across_cycles() { /* run 2 cycles; decisions' RNG stream continuous (no reseed: cycle2 differs from a fresh-seed cycle) */ }
#[test] fn dryrun_cycle_appends_decisions_to_journal_and_state_jsonl() { /* tmpdir journal grows */ }
#[test] fn cycle_skips_and_logs_when_min_competitors_not_3() {}
#[test] fn no_setchannel_symbol_in_crate() { /* compile-time guard: grep-style source scan test over crates/revops/src asserting "setchannel" absent (Global Constraint) */ }
```

- [ ] **Step 2:** `cargo test -p revops --test fee_scheduler` → FAIL.
- [ ] **Step 3: Implement** scheduler + `main.rs` wiring.
- [ ] **Step 4:** `cargo test --workspace` (dev) AND `cargo test --workspace --release` (float-hardening leg) → PASS; clippy clean.
- [ ] **Step 5: Commit.** `git commit -m "feat(wiring): single-owner fee-cycle scheduler, one PyRandom, one clock/cycle, revops-r-fee-dryrun default OFF"`

---

### Task 7: `revenue-r-fee-debug` RPC + wake/policy triggers (checklist item 7)

**Files:**
- Modify: `crates/revops/src/fee_scheduler.rs` (extend `CycleMsg` + owner-thread handlers), `crates/revops/src/main.rs` (RPC registration + notification hooks)
- Test: `crates/revops/tests/fee_scheduler.rs` (extend)

**Interfaces:**
- Consumes: T6's `SchedulerHandle`/`CycleMsg::Query`; `revops_fees::cycle::{wake_all_sleeping_channels, maybe_wake_for_vegas_spike, handle_policy_change}`; `ControllerState::dts_summary`; `rpc_name("fee-debug")`.
- Produces: RPC `revenue-r-fee-debug` (params: optional `channel_id`) returning Python `revenue-fee-debug`'s shape for one channel (`dts_summary`) or the controller summary (`last_decision_summary` + per-channel map) — field names byte-matching Python's response (cite `cl-revenue-ops.py`'s `revenue-fee-debug` handler; the diff harness for read-RPCs can then be pointed at it later). Extended `CycleMsg`:

```rust
pub enum CycleMsg {
    RunCycleNow,
    PolicyChanged { peer_id: String },   // -> handle_policy_change(...)
    VegasSpikeCheck,                     // -> maybe_wake_for_vegas_spike(...)
    WakeAll,                             // -> wake_all_sleeping_channels(...)
    Query(FeeDebugQuery, std::sync::mpsc::Sender<serde_json::Value>),
    Shutdown,
}
```

Wiring of triggers in `main.rs`: a `setconfig`-driven policy change isn't observable cross-plugin during the window, so the two live triggers are (a) a manual `revenue-r-fee-wake` RPC (maps to `WakeAll` — operator/diagnostic use, mirrors Python's wake semantics) and (b) a `VegasSpikeCheck` sent by the ticker between full cycles (Python checks vegas spikes off its HTLC monitor; the ticker cadence is the dry-run-faithful stand-in — document this delta in the RPC's doc comment as a cutover watch item). `PolicyChanged` is sent by nothing yet but is the seam the cutover's policy-RPC will use — constructing it now keeps T7's enum the stable contract.

- [ ] **Step 1: Failing tests:** `fee_debug_query_returns_dts_summary_shape` (byte-compare against a committed fixture generated from Python's `get_dts_summary` on the same state blob); `wake_all_msg_clears_sleep_state`; `vegas_spike_check_respects_wake_armed_edge_trigger`.
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement** handlers + RPC registration (`.rpcmethod(&rpc_name("fee-debug"), ...)`, same pattern as `revenue-r-status`).
- [ ] **Step 4:** `cargo test --workspace` both profiles → PASS; clippy clean.
- [ ] **Step 5: Commit.** `git commit -m "feat(wiring): revenue-r-fee-debug RPC + wake/policy CycleMsg triggers"`

---

### Task 8: Pre-dry-run verifications, deploy, live window (checklist items 8 + PRE-DRY-RUN VERIFICATIONS) — CONTROLLER-RUN

**Files:**
- Create: `docs/runbooks/fee-dryrun-deploy.md` (the deploy section below, expanded with live-verified outputs as the observer runbook did)

This task mirrors `docs/runbooks/observer-deploy.md` (build → ship → plugin restart → verify → window → rollback) and is executed by the controller with operator awareness — lnnode is production.

- [ ] **Step 1 — PRE-DRY-RUN VERIFICATION (a): journal path explicit.** Decide and record the exact journal dir (`/data/lightningd/.lightning/`, next to `revops-r-observer.db`); confirm `tools/diff-harness/diff_fee_decisions.py`'s `DEFAULT_JOURNAL_PATH` (T3) equals `<that dir>/fee_dryrun_journal.jsonl`. Never rely on the empty-default inference.
- [ ] **Step 2 — PRE-DRY-RUN VERIFICATION (b): lnnode `neighbor_median_min_competitors`.** `ssh lnnode 'lightning-cli revenue-config get neighbor_median_min_competitors'` — expect `3`. If not 3: STOP; either the operator resets it or a plumbing task is added; the T1/T6 fail-closed guard will refuse cycles regardless.
- [ ] **Step 3 — PRE-DRY-RUN VERIFICATION (c): chain-costs falsiness mapping.** Run T2's `chain_costs_falsiness_maps_to_none` against a fresh copy of the LIVE row shape: `ssh lnnode 'sqlite3 -readonly /data/lightningd/.lightning/revenue_ops.db "SELECT ..."'` for the chain-costs source table; confirm the mapping doc comment matches what production actually stores.
- [ ] **Step 4 — PRE-DRY-RUN VERIFICATION (d): `fee_changes` strict-parse watch items.** Spot-query production `fee_changes` for the known watch items (reconcile timestamp coercion, `json_i64_or_zero` string coercion, policy int-vs-float rendering — Phase 2/3 riders): `sqlite3 -readonly ... "SELECT typeof(timestamp), typeof(old_fee_ppm), typeof(new_fee_ppm) FROM fee_changes ORDER BY id DESC LIMIT 50;"` — any non-INTEGER type is triaged BEFORE the window opens (harness would misparse silently otherwise).
- [ ] **Step 5 — Build + ship (runbook §1 pattern):**

```sh
rustup show                                  # 1.97 per rust-toolchain.toml
cargo test --workspace --release             # release parity leg MUST be green
cargo build --release -p revops
ldd target/release/revops                    # compat check vs lnnode glibc 2.41 / libsqlite3.so.0
scp target/release/revops lnnode:/home/lightningd/revops-r-deploy/revops.new
ssh lnnode 'chmod +x /home/lightningd/revops-r-deploy/revops.new'
```

- [ ] **Step 6 — Plugin restart with dry-run ON (runbook §2 pattern; stop-then-start is the "restart"):**

```sh
ssh lnnode 'lightning-cli plugin stop /home/lightningd/revops-r-deploy/revops || true'
ssh lnnode 'mv /home/lightningd/revops-r-deploy/revops.new /home/lightningd/revops-r-deploy/revops'
ssh lnnode "lightning-cli -k plugin subcommand=start \
  plugin=/home/lightningd/revops-r-deploy/revops \
  revops-r-db-path=/data/lightningd/.lightning/revenue_ops.db \
  revops-r-observer-db-path=/data/lightningd/.lightning/revops-r-observer.db \
  revops-r-journal-dir=/data/lightningd/.lightning \
  revops-r-fee-dryrun=true"
lightning-cli revenue-r-ping        # {"pong": true, ...}
lightning-cli revenue-r-fee-debug   # controller summary present after first cycle
ssh lnnode 'ls -la /data/lightningd/.lightning/fee_dryrun_journal.jsonl'   # appears after first cycle (fee_interval=1800s default)
```

- [ ] **Step 7 — Live window (checklist item 8):** let the journal accumulate over several hours (≥ 6 Python fee cycles), then:

```sh
python3 tools/diff-harness/diff_fee_decisions.py --node lnnode \
  --since <window_start_ts> --until <window_end_ts>
```

**GATE: zero deterministic mismatches.** Skips must discriminate via `would_broadcast`/`algorithm_values: null` (never `reason_code` alone). Every MISMATCH is triaged to (a) porting bug → fix task, or (b) documented non-determinism (timing skew across the 120s tolerance) with evidence. Repeat over a second bounded window after any fix. Record window bounds, counts, and verdict in `.superpowers/sdd/progress.md`.
- [ ] **Step 8 — Rollback (always available):** `ssh lnnode 'lightning-cli plugin stop /home/lightningd/revops-r-deploy/revops'` — Python untouched throughout (observer-runbook §5 guarantees hold: the journal and dry-run ledger are Rust-owned files; delete them for a clean slate).
- [ ] **Step 9 — Commit runbook** `git add docs/runbooks/fee-dryrun-deploy.md && git commit -m "docs: fee dry-run deploy runbook + window verdict"`

---

## Deferred to Cutover (checklist item 9 — explicit, not silent)

- `setchannel` + dynamic-htlcmax broadcast behind the cutover flag (the ONLY task allowed to introduce a `setchannel` call; removes T6's source-scan guard test in the same commit).
- `forward_event` failed-forward fee-nudge hook wiring (pure nudge math already ported, Phase 4 T7).
- `set_initial_fee` full RPC path.
- Seed-once `StateLifecycle` flip + `StateSink` to the Rust-owned writable DB (Design Note 1's consequence).
- Rust-side mempool recorder (T2's `mempool_ma_24h` dependency note).
- 3B riders that bind at fee/rebalance cutover: journal-hook category+amount resolution at `budget.rs:298`, `_DATASTORE_MAX_BYTES=60000` pin, `checked_add` money-path audit.
