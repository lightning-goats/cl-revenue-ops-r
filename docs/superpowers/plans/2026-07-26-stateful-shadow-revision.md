# Stateful Shadow Revision Implementation Plan (2026-07-26)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring the partially-implemented stateful-shadow branch (`codex/stateful-shadow-cutover`, original plan Tasks 2–3 complete) up to current `main`, fold in everything learned since Jul 20 (the pre-decision-epoch fix, the live decision-surface engagement gate, deep-audit iteration 1), land the remaining decision-path audit lows on this candidate, then continue the original plan's Tasks 4–12 with the amendments listed here.

**Architecture:** Unchanged from `docs/superpowers/plans/2026-07-20-rust-stateful-shadow-and-live-adapter.md` (pure kernel → typed intents → capability-free recorder → transactional Rust-owned store; arm-gated live adapter). This revision REBASES that work, adds a decision-path fix batch that must ride this candidate (any decision fix resets the 72-hour soak, so they ship together), pins the SeedOnce epoch-identity invariant discovered by the 2026-07-23 gate-starvation incident, and adapts the engagement gate to the new journal store.

**Tech Stack:** identical to the original plan (Rust 1.97, serde/serde_json, rusqlite via the single-owner actor, cln-rpc, sha2, Tokio, tempfile).

## Global Constraints

All Global Constraints of `2026-07-20-rust-stateful-shadow-and-live-adapter.md` apply verbatim, plus:

- Baseline is current `origin/main` (`1af60fb` or later) — NOT `bc8c9be`. The branch must be rebased before any new work (Task R1).
- Preserve the pre-decision-epoch contract (`993632d`): every epoch-derived read in `adjust_channel_fee` consumes `AdjustCtx::pre_last_update`. Under SeedOnce the epoch and the hydrated `cycle.last_update` must be IDENTICAL by construction — Task R5 pins this with a test; breaking it re-opens the gate-starvation failure invisibly.
- The autonomous lane must keep emitting, per decision row: cycle ts, channel id, `would_broadcast`, `algorithm_values`-null-ness, trace `disposition`, and the `skip_gate_comparable` marker — `tools/diff-harness/engagement_gate.py` consumes exactly these (Task R7 adapts its source; the fields themselves are load-bearing).
- The seed import (SeedOnce's one-time read of Python state) fails CLOSED: any field where Python's own `from_dict` would raise refuses the seed and leaves the plugin passive-observer; a refused seed is red runway evidence, never a silent fresh-state fallback.
- Decision-path audit lows (Tasks R2–R4) land on this branch BEFORE the SeedOnce work, so the whole candidate soaks once.
- Low-batch 1 (`44b536c`) is already on `main`; the rebase inherits it. Do not re-implement.

---

### Task R1: Rebase the branch onto current main

**Files:**
- No new files. Conflict resolution only.

**Interfaces:**
- Produces: branch `codex/stateful-shadow-cutover` rebased onto `origin/main`, full workspace green, replay byte-exact.

- [ ] **Step 1: Rebase.**

  ```bash
  cd /home/sat/bin/cl-revenue-ops-r/.worktrees/stateful-shadow-cutover
  git fetch origin
  git rebase origin/main
  ```

  Expected conflicts and their resolutions (the four branch commits touch files that 15 main commits also changed):

  - `crates/revops-fees/src/cycle.rs`: main added `node_drain_bias_max` (H2), `missing_channel_info` tally + `SKIP_REASON_ORDER` (M4/F3), and the `AdjustCtx::pre_last_update` epoch plumbing (`993632d`). The branch changed `StateSink` to fallible and threaded `PreparedFeeAction`. Keep BOTH: main's field/tally/epoch code is authoritative for decision inputs; the branch's sink/executor signatures are authoritative for persistence. Where `run_fee_cycle`'s flush block conflicts, the merged form propagates the sink error (branch) from the block that iterates `dirty` (main's shape).
  - `crates/revops-fees/src/execution.rs`: main did not touch `SetChannelRequest` regions; conflicts here should be contextual only — take both sides.
  - `crates/revops-fees/tests/cycle.rs`: main added `NodeChannel`/`since_log` fields to `SyntheticEvidence` and new tests; the branch added sink tests. Merge both; every `SyntheticEvidence` literal the branch added gains `node_channels: Vec::new(), since_log: Default::default(),`.

- [ ] **Step 2: Full verification of the rebased branch.**

  ```bash
  cargo fmt --all -- --check
  cargo test --workspace
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test -p revops-fees --test replay --test replay_wire
  cargo test -p revops-fees --test cycle decision_gate_uses_pre_decision_epoch_not_fresh_flush
  ```

  Expected: everything green — in particular the epoch test and byte-exact replay, proving the rebase kept both the branch's fallible sink AND main's epoch fix intact.

- [ ] **Step 3: Force-push the rebased branch.**

  ```bash
  git push --force-with-lease origin codex/stateful-shadow-cutover
  ```

---

### Task R2: Panic/overflow hardening trio (audit lows, decision path)

**Files:**
- Modify: `crates/revops-fees/src/thompson/dynamics.rs` (`supported_fee_ceiling`, ~line 244)
- Modify: `crates/revops-fees/src/state_store.rs` (`load_cycle_state` clamp block, ~line 883)
- Modify: `crates/revops-econ/src/ledger.rs` (~line 342)
- Test: `crates/revops-fees/tests/thompson_dynamics.rs`, `crates/revops-fees/tests/state_serde.rs`, `crates/revops-econ/tests/ledger.rs`

**Interfaces:**
- Consumes: existing `GaussianThompsonState`, `load_cycle_state`, ledger replay.
- Produces: no signature changes; corrupt-input behavior only.

- [ ] **Step 1: Failing test — NaN observation fee must not panic `supported_fee_ceiling`.**

  In `crates/revops-fees/tests/thompson_dynamics.rs`:

  ```rust
  /// Audit low (2026-07-22): a corrupt/hand-edited blob can carry a NaN
  /// observation fee (pyjson deliberately parses the NaN literal, and
  /// serde passes fees through unsanitized, as Python does). Python's
  /// sorted() tolerates NaN; the Rust comparator panicked and killed the
  /// plugin. Corrupt-only input: deterministic non-crash ordering
  /// (total_cmp, NaN last) is the contract, not byte parity.
  #[test]
  fn supported_fee_ceiling_survives_nan_observation_fee() {
      let mut state = GaussianThompsonState::default();
      state.observations = vec![
          obs(300.0, 120.0, 1.0, NOW - 3600),
          obs(f64::NAN, 5.0, 1.0, NOW - 7200), // positive revenue, finite mass
          obs(500.0, 90.0, 1.0, NOW - 1800),
      ];
      // Must return without panicking; the exact value is NOT pinned
      // (corrupt-input, documented divergence).
      let _ = state_ceiling(&state, NOW, None);
  }
  ```

  (`obs`/`state_ceiling` follow the file's existing helper names — reuse them; if the file drives `supported_fee_ceiling` through a different helper, call that one.)

- [ ] **Step 2: Run to verify it fails.** `cargo test -p revops-fees --test thompson_dynamics supported_fee_ceiling_survives_nan` — expected: panic "fee is never NaN".

- [ ] **Step 3: Fix — total order instead of `expect`.**

  ```rust
  // Corrupt-only divergence: Python's sorted() tolerates a NaN fee
  // without raising (arbitrary placement); Rust orders NaN last via
  // total_cmp. Reachable only from a hand-corrupted blob — the plugin
  // must degrade, not die (audit low, 2026-07-22).
  masses.sort_by(|a, b| a.0.total_cmp(&b.0));
  ```

- [ ] **Step 4: Failing test — poisoned `last_broadcast_fee_ppm` clamped at load.**

  In `crates/revops-fees/tests/state_serde.rs` (alongside the existing P2 clamp tests):

  ```rust
  /// Audit low (2026-07-22): `resync_broadcast_fee` computes
  /// `(actual - tracked).abs()`, which overflows (debug panic) when a
  /// hand-corrupted blob carries last_broadcast_fee_ppm near i64::MIN.
  /// The P2 hardening clamped pending_target_ppm and
  /// congestion_entry_fee_ppm at load; the broadcast/last fee fields get
  /// the same [0, ABS_MAX_FEE_PPM] clamp.
  #[test]
  fn load_cycle_state_clamps_poisoned_fee_fields() {
      let blob = serde_json::json!({
          "last_broadcast_fee_ppm": i64::MIN + 5,
          "last_fee_ppm": -7,
      });
      let state = load_cycle_state_from_json(&blob);
      assert_eq!(state.last_broadcast_fee_ppm, 0);
      assert_eq!(state.last_fee_ppm, 0);
  }
  ```

  (Adapt the constructor call to the file's existing load-path test helper.)

- [ ] **Step 5: Run to verify it fails, then fix.** In `load_cycle_state`:

  ```rust
  last_fee_ppm: get_i64("last_fee_ppm", 0).clamp(0, ABS_MAX_FEE_PPM),
  last_broadcast_fee_ppm: get_i64("last_broadcast_fee_ppm", 0).clamp(0, ABS_MAX_FEE_PPM),
  ```

  NOTE: Python does not clamp these (bigints can't overflow, so it has no
  need); the clamp is reachable only from values Python can never write
  (its own writers bound fees ≤ 100_000). Replay fixtures carry
  Python-written values and are unaffected — verify with the replay suite
  in Step 8.

- [ ] **Step 6: Failing test — ledger reservation subtraction fails closed.**

  In `crates/revops-econ/tests/ledger.rs`:

  ```rust
  /// Audit low F1 (2026-07-22): the one unchecked arithmetic site in
  /// replay. A cost_recorded event with cost_msat near i64::MIN against a
  /// large positive reservation overflows `cur_reserved - cost`. Python's
  /// bigint `max(0, reserved - cost)` cannot overflow; Rust fails CLOSED
  /// like the checked_add three lines above.
  #[test]
  fn replay_reservation_release_overflow_fails_closed() {
      let events = vec![
          reserve_event("k", i64::MAX - 10),
          cost_event("k", i64::MIN + 2),
      ];
      assert!(replay(&events).is_err());
  }
  ```

  (Use the file's existing event-builder helpers.)

- [ ] **Step 7: Run to verify it fails (debug panic), then fix.**

  ```rust
  let new_reserved = cur_reserved.checked_sub(cost).ok_or_else(|| EconError {
      msg: format!(
          "ledger replay: reservation release overflow for {key}: \
           {cur_reserved} - {cost}"
      ),
  })?;
  reserved.insert(key.clone(), new_reserved.max(0));
  ```

- [ ] **Step 8: Full-suite verify and commit.**

  ```bash
  cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings
  cargo test -p revops-fees --test replay --test posterior
  git add crates/ && git commit -m "fix: harden corrupt-blob panic/overflow sites (audit lows)"
  ```

---

### Task R3: Config parity pair (audit lows, decision path)

**Files:**
- Modify: `crates/revops/src/fee_config.rs` (`resolve_string` callers for `fee-profile`, `market-fee-mode`; `resolve_fee_cfg` tail)
- Test: `crates/revops/tests/fee_config.rs`

**Interfaces:**
- Consumes: `python_startup_bool` precedent (M2) for layer-aware casting; `FeeCfgSnapshot`.
- Produces: `resolve_fee_cfg` output additionally lowercased/repaired; no signature change.

- [ ] **Step 1: Failing tests — layer-(b) string normalization.**

  ```rust
  /// Audit low #10a: Python lowercases fee_profile and market_fee_mode at
  /// startup (cl-revenue-ops.py:2514,2517 str(...).lower()); an operator
  /// config `revenue-ops-fee-profile=Active` selects profile "active" in
  /// Python but missed the Rust lookup and silently fell to the default
  /// profile — different sleep/step parameters every cycle.
  #[tokio::test]
  async fn resolve_fee_cfg_lowercases_profile_and_market_mode_from_listconfigs() {
      let mut py = HashMap::new();
      py.insert("revenue-ops-fee-profile".to_string(),
                cln_plugin::options::Value::String("Active".to_string()));
      py.insert("revenue-ops-market-fee-mode".to_string(),
                cln_plugin::options::Value::String("UNDERCUT".to_string()));
      let cfg = revops::fee_config::resolve_fee_cfg(None, &py).await;
      assert_eq!(cfg.fee_profile, "active");
      assert_eq!(cfg.market_fee_mode, "undercut");
  }
  ```

- [ ] **Step 2: Failing tests — cross-field contradiction repairs (the two fee-cycle-input pairs).**

  ```rust
  /// Audit low #9b: Python's load_overrides repairs crossed pairs after
  /// applying overrides (config.py:946-951 min>max -> min=max;
  /// config.py:975-980 receivable floor>target -> floor=target).
  /// Reachable via manual DB edits/TOCTOU; Rust used the raw crossed
  /// values in the fee cycle.
  #[tokio::test]
  async fn resolve_fee_cfg_repairs_crossed_min_max() {
      let (handle, _tmp) = fixture_db_with_override("min-fee-ppm", "3000").await;
      // max stays default 2000 -> crossed -> min repaired to 2000.
      let cfg = revops::fee_config::resolve_fee_cfg(Some(&handle), &HashMap::new()).await;
      assert_eq!(cfg.min_fee_ppm, cfg.max_fee_ppm);
  }

  #[tokio::test]
  async fn resolve_fee_cfg_repairs_crossed_receivable_band() {
      let (handle, _tmp) =
          fixture_db_with_override("receivable-ratio-floor", "0.6").await;
      // target stays default 0.30 -> floor repaired to target.
      let cfg = revops::fee_config::resolve_fee_cfg(Some(&handle), &HashMap::new()).await;
      assert_eq!(cfg.receivable_ratio_floor, cfg.receivable_ratio_target);
  }
  ```

- [ ] **Step 3: Run both to verify failure, then implement.**

  In `resolve_fee_cfg`: for the two string fields, wrap the resolved
  layer-(b)/(c) value with `.to_lowercase()` mirroring the startup cast —
  but NOT the layer-(a) DB value, which `validate_override`'s enum gate
  already lowercases (keep the layer split the same way `resolve_bool`
  does post-M2). At the end of `resolve_fee_cfg`, before returning:

  ```rust
  // Python load_overrides post-load repairs (config.py:946-951, 975-980)
  // for the two crossed pairs that are fee-cycle inputs. Warn-log like
  // Python; repair identically.
  if cfg.min_fee_ppm > cfg.max_fee_ppm {
      eprintln!(
          "revops: contradictory min_fee_ppm ({}) > max_fee_ppm ({}); repaired min to max",
          cfg.min_fee_ppm, cfg.max_fee_ppm
      );
      cfg.min_fee_ppm = cfg.max_fee_ppm;
  }
  if cfg.receivable_ratio_floor > cfg.receivable_ratio_target {
      eprintln!(
          "revops: contradictory receivable_ratio_floor ({}) > target ({}); repaired floor to target",
          cfg.receivable_ratio_floor, cfg.receivable_ratio_target
      );
      cfg.receivable_ratio_floor = cfg.receivable_ratio_target;
  }
  ```

- [ ] **Step 4: Verify, full suite, commit.**

  ```bash
  cargo test -p revops && cargo test --workspace --quiet
  git add crates/revops && git commit -m "fix(plugin): profile lowercasing and crossed-pair repairs (audit lows)"
  ```

---

### Task R4: Thompson serde round-trip fidelity + non-panicking render

**Files:**
- Modify: `crates/revops-fees/src/thompson/serde.rs`
- Modify: `crates/revops-econ/src/intents.rs` (`render_component_value`)
- Test: `crates/revops-fees/tests/state_roundtrip.rs`, `crates/revops-econ/tests/intents.rs`

**Interfaces:**
- Consumes: `revops_econ::pyfloat::py_repr` (floats render as Python `str(float)` — identical to `repr`, so this is BYTE PARITY, not a lossy degrade).
- Produces: `Explanation::render()` never panics; observation tuples round-trip verbatim.

- [ ] **Step 1: Failing tests — lossless observation round-trip.**

  ```rust
  /// Audit lows F4/F5: a 6-element observation whose 6th element is not a
  /// string was silently re-emitted as 5 elements (and >=7-element tuples
  /// shifted); int-typed posterior_mean/std re-emitted as floats. Python
  /// round-trips both verbatim. Matters for SeedOnce: the seed import
  /// must not rewrite Python-written state it will later be compared to.
  #[test]
  fn observation_with_non_string_sixth_element_roundtrips_verbatim() {
      let d = state_dict_with_observation(json!([300, 12.5, 1.0, 1_752_400_000, "normal", 7, "x"]));
      let state = gts_from_dict(&d).unwrap();
      assert_eq!(gts_to_dict(&state)["observations"][0],
                 json!([300, 12.5, 1.0, 1_752_400_000, "normal", 7, "x"]));
  }

  #[test]
  fn int_typed_posterior_mean_and_std_keep_json_typing() {
      let d = state_dict_with(json!({"posterior_mean": 200, "posterior_std": 50}));
      let state = gts_from_dict(&d).unwrap();
      let out = gts_to_dict(&state);
      assert!(out["posterior_mean"].is_i64(), "got {}", out["posterior_mean"]);
      assert!(out["posterior_std"].is_i64());
  }
  ```

  (Follow `state_roundtrip.rs`'s existing dict-builder helpers; the
  mechanism mirrors the existing `prior_mean_fee_is_int`/`fee_is_int`
  flags — extend the same pattern, do not invent a new one.)

- [ ] **Step 2: Run to verify failure, implement via the `*_is_int` flag pattern plus verbatim `extra` capture that starts at index 5 when the element is not a string.**

- [ ] **Step 3: Failing test — float component renders as Python.**

  ```rust
  /// Audit low F8: render() panicked on float components as a Phase-2
  /// tripwire. py_repr IS Python's str(float) (== repr in py3), so
  /// rendering through it is byte parity — the tripwire can retire
  /// before Phase-2b wiring makes the panic reachable.
  #[test]
  fn render_formats_float_components_like_python() {
      let e = Explanation::new("cycle_rebalance",
          vec![("score".into(), json!(0.30000000000000004))]);
      assert_eq!(e.render(), "cycle_rebalance: score=0.30000000000000004");
  }
  ```

- [ ] **Step 4: Run to verify panic, then replace the panic arm:**

  ```rust
  Value::Number(n) => {
      if let Some(f) = n.as_f64().filter(|_| n.is_f64()) {
          // Python renders float components via str(float) == repr;
          // py_repr is the pinned byte-exact implementation.
          revops_econ::pyfloat::py_repr(f)
      } else {
          n.to_string()
      }
  }
  ```

  (Adjust for crate: `render_component_value` lives IN revops-econ — call
  `crate::pyfloat::py_repr`.) Remove the doc comment's panic contract and
  the corresponding "wiring trap" warnings in `shadow.rs`/`intents.rs`.

- [ ] **Step 5: Verify, full suite, commit.**

  ```bash
  cargo test --workspace --quiet && cargo clippy --workspace --all-targets -- -D warnings
  git add crates/ && git commit -m "fix: serde round-trip fidelity and parity float render (audit lows)"
  ```

---

### Task R5: Pin the SeedOnce epoch-identity invariant

**Files:**
- Test: `crates/revops/tests/fee_scheduler.rs` (or `crates/revops-fees/tests/cycle.rs` if the lifecycle seam is reachable there)

**Interfaces:**
- Consumes: `StateLifecycle::SeedOnce` (original plan Task 5), `ControllerState::skip_gate_prev`, `AdjustCtx::pre_last_update`.
- Produces: a regression test the SeedOnce implementation must keep green.

- [ ] **Step 1: When original-plan Task 5 lands, add this test in the same commit** (it cannot compile earlier — the lifecycle does not exist yet; that is why it sits in this plan as a REQUIREMENT on Task 5's implementer):

  ```rust
  /// 2026-07-23 gate-starvation lesson: in shadow-RehydratePerCycle the
  /// hydrated last_update is Python's POST-decision flush and the T8b
  /// pre-decision epoch differs from it; the decision gate must consume
  /// the T8b epoch (commit 993632d). Under SeedOnce, Rust owns the state:
  /// last_update is written by Rust's own previous cycle, so the two
  /// epochs MUST be identical — if they ever diverge, an epoch bug has
  /// been reintroduced where the engagement gate can no longer see it.
  #[test]
  fn seedonce_pre_decision_epoch_equals_owned_last_update() {
      let h = seedonce_harness_with_one_channel();
      h.run_cycle(); // cycle 1: seeds + writes Rust-owned state
      h.run_cycle(); // cycle 2: hydrates Rust's own state
      let cached = h.state().skip_gate_prev.get("700x1x0").unwrap().last_update;
      let owned = h.state().cycle_states.get("700x1x0").unwrap().last_update;
      assert_eq!(cached, owned,
          "SeedOnce epochs diverged: the decision gate and the hydrated \
           state disagree about the pre-decision timestamp");
  }
  ```

  (`seedonce_harness_with_one_channel` is whatever harness Task 5 builds
  for its own restart tests — reuse it; the assertion is the deliverable.)

- [ ] **Step 2: Run, verify green, and fold into Task 5's commit.**

---

### Task R6: Fail-closed SeedOnce seed import

This AMENDS original-plan Task 5 (do not implement Task 5 without it):

- The one-time seed of Rust state from Python's `fee_strategy_state` uses
  the EXACT `from_dict` parity path. Any field where Python's own
  `from_dict` would raise (see audit F6: non-numeric `_last_fee_min`,
  string rows inside `posterior_precision`, dict entries in
  `posterior_bias`) REFUSES the whole seed: the plugin logs the offending
  channel + field, stays in passive-observer mode, and records a
  `seed_refused` row in the Rust-owned store. No partial seed, no silent
  fresh-state fallback — a refused seed is red runway evidence.
- The seed records provenance: source DB path, `MAX(last_update)` at seed
  time, row count, sha256 of the serialized seed payload, and the binary's
  source commit — queryable via `revenue-r-status` for the runway
  controller.
- Add to Task 5's test list: a corrupt-field seed refusal test per F6
  class, plus a provenance readback test.

---

### Task R7: Engagement gate reads the Rust-owned store

**Files:**
- Modify: `tools/diff-harness/engagement_gate.py`

**Interfaces:**
- Consumes: the shadow-cycle-outcome table original-plan Task 4 creates. COLUMN CONTRACT (Task 4's implementer must provide exactly these, however the table is otherwise shaped): `cycle_ts INTEGER`, `channel_id TEXT`, `would_broadcast INTEGER`, `has_algorithm_values INTEGER`, `disposition TEXT`, `skip_gate_comparable INTEGER`.
- Produces: `--source sqlite --observer-db <path>` mode; JSONL mode unchanged (the gate must keep measuring OLD candidates' journals during the transition).

- [ ] **Step 1: Extend the self-test first** with a stubbed sqlite fetch returning the same scenarios the JSONL stub covers (green shape, starved red shape, low-sample yellow, non-comparable exclusion) and assert identical verdicts across both sources.

- [ ] **Step 2: Run self-test to verify the new cases fail** (`--source` unknown), then implement: an `--source jsonl|sqlite` flag (default `jsonl`), a `fetch_sqlite_rows()` that maps the column contract into the same decision-dict shape `collect_cycles()` consumes, and identical metric code downstream (no metric logic may fork on source).

- [ ] **Step 3: Verify self-test green, run once live against the CURRENT candidate's JSONL to confirm no regression, commit.**

  ```bash
  python3 tools/diff-harness/engagement_gate.py --self-test
  python3 tools/diff-harness/engagement_gate.py --node lnnode --since 1784830835
  git add tools/diff-harness/engagement_gate.py
  git commit -m "feat(diff-harness): sqlite source for the engagement gate"
  ```

---

### Task R8: Continue original-plan Tasks 4–12, with this amendments ledger

Execute `2026-07-20-rust-stateful-shadow-and-live-adapter.md` Tasks 4–12
in order, applying these deltas (each is a correction to the original
text, discovered since Jul 20):

1. **Task 4 (schema):** add the engagement-gate column contract from Task
   R7 to the shadow-cycle-outcome table. The disposition strings are the
   ones `adjust_channel_fee` emits today (`waiting_window`,
   `sleeping_hold`, `alpha_guard`, `gossip_suppressed`, `broadcast`,
   `gossip_refresh`) — store them verbatim, do not re-encode.
2. **Task 5 (SeedOnce):** apply Task R5's epoch-identity test and Task
   R6's fail-closed seed. The scheduler signatures changed on main:
   `spawn`/`spawn_with_thread_spawner`/`trigger_loop` take
   `PythonOptionCache` (M3), not `HashMap<String, OptValue>`; the
   per-cycle `dispatch_cycle` refresh stays — SeedOnce changes state
   ownership, NOT config-layer semantics.
3. **Task 6 (mempool/triggers):** unchanged, except trigger receipts must
   share the cycle-ts keying the engagement gate uses.
4. **Task 8 (mode matrix):** unchanged table; add one row-level assertion
   that `fee-stateful-shadow=true` REQUIRES the seed provenance row to
   exist (a stateful shadow that never seeded is a misconfiguration, fail
   startup).
5. **Task 12 (staging):** the deployment procedure is now the checksummed
   swap in `docs/audit/2026-07-19-shadow-parity-deployment-closeout.md`
   as exercised on Jul 22/23 (staged binary + hash verify + rollback copy
   + `plugin stop`/`mv`/`plugin start` with keyword args). Post-deploy
   verification adds: first-cycle bootstrap is expected non-comparable;
   the engagement gate (Task R7 sqlite mode) must go green from cycle 2.
   Per the runway candidate rules the deployment starts a fresh 72-hour
   soak measured daily by `engagement_gate.py`.

**Companion-plan deltas** (do not execute here; note for their executors):

- `2026-07-20-shadow-runway-timers-and-deployment.md`: the daily-rollup
  unit invokes `engagement_gate.py --source sqlite` and maps exit codes
  0/3/1/2 to green/yellow/red/transport in the controller report.
- `2026-07-20-python-fee-authority-handoff.md`: unchanged; still the
  prerequisite for original-plan Tasks 8–9 (authority readback). It can
  proceed in parallel with R1–R7.

---

## Completion Evidence

- Rebased branch green: fmt, clippy `-D warnings`, full workspace tests, byte-exact replay.
- Audit-low batch commits (R2, R3, R4) each with watched-red tests.
- SeedOnce epoch-identity and seed-refusal tests green (R5, R6, folded into Task 5).
- `engagement_gate.py --self-test` green with both sources; live JSONL run non-regressed (R7).
- Original-plan Tasks 4–12 completion evidence as specified there, with the R8 amendments applied.
- No deployment from this plan without the runway's candidate rules: new binary = new candidate = fresh 72-hour soak.
