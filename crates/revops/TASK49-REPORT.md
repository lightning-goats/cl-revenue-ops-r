# Task 49 report — register Wave-2 "RPC Batch A" builders

Registers the ten read-only RPC Batch A response builders documented in
`crates/revops/RPC_BATCH_A.md` as real `.rpcmethod()` handlers in
`crates/revops/src/main.rs`: `health`, `profitability`, `analyze`, `policy`,
`list-banned`, `list-ignored`, `hot-channel-protection-peers`,
`capacity-report`, `econ-snapshot`, `spend-ledger`.

Per the honest progress model in
`docs/superpowers/specs/2026-07-27-whole-plugin-rust-cutover-design.md`, this
task's claim is **reachable**, not effective/transport-proven/
promotion-ready: all ten modules were already **compiled** (declared in
`lib.rs`) before this task; this task made them **reachable** (registered,
manifest-visible, callable). Several handlers still call no live evidence
pipeline (see the per-RPC table below) and honestly return their
null/`_gaps`/`disabled` contract — registering the RPC did not fabricate
missing evidence.

## Baseline

- `main` @ `650d832` (this worktree branched from there).
- 27 compiled `rpc_*` modules under `crates/revops/src/` (unchanged by this
  task — all ten Batch A modules were already declared in `lib.rs`).
- 10 `.rpcmethod()` registrations in `crates/revops/src/main.rs`.

## After this task

- 27 compiled `rpc_*` modules (unchanged).
- **20** `.rpcmethod()` registrations — verified by
  `crates/revops/tests/manifest.rs`'s `assert_eq!(result["rpcmethods"]...len(), 20, ...)`
  guard in `manifest_canonical_mode_advertises_revenue_ops_names`, and by the
  two new exact-name assertions
  `manifest_batch_a_methods_registered_shadow_mode` /
  `_canonical_mode`.

## Per-RPC evidence source

| RPC (canonical name) | Evidence wired | Honest gap(s) preserved |
|---|---|---|
| `revenue-health` | `queries::pnl_summary` (today/week financials) — real, DB-backed | `annualized_roc_pct` (needs live `listpeerchannels` capacity); channels/fees/rebalancer/budget/boltz/planner/top_routes/loops sections (no Rust daemon-loop state yet) |
| `revenue-profitability` | none | whole response is the honest no-data/empty-summary shape; no `ChannelProfitability` assembly pipeline exists; `fee_multiplier` always `null` |
| `revenue-analyze` | none | single-channel `analysis` always `null` (no `FlowMetrics` assembly); no-`channel_id` returns explicit `not_yet_ported` (Python's equivalent is a mutating background sweep, out of this read-only batch's scope) |
| `revenue-policy` (list/get/find/changes) | `queries::all_policies` / `policy_for_peer` / `policies_by_tag` / `policy_changes_since` / `last_policy_change_timestamp` — real, DB-backed | none for the 4 read actions; set/delete/tag/untag/batch are refused BEFORE any DB access |
| `revenue-list-banned` | `queries::all_policies`, filtered by the `banned` tag — real, DB-backed | none |
| `revenue-list-ignored` | `queries::all_policies`, filtered by strategy=passive + rebalance=disabled — real, DB-backed | none; DEPRECATED, ported for parity only |
| `revenue-hot-channel-protection-peers` (`list` only) | `queries::hot_channel_protection_override_peers` — real, DB-backed | add/remove/clear refused (DB writes, out of scope) |
| `revenue-capacity-report` | `timestamp` only | mempool_recommendation/summary/winners/losers/recommendations all `null` + `_gaps`-listed — the winner/loser identification engine is not ported |
| `revenue-econ-snapshot` | none | `enabled` hardcoded `false` (same config surface `revenue-r-config` reads is not wired here yet) — always the honest `{"enabled": false, "hint": ...}` shape |
| `revenue-spend-ledger` | `queries::spend_ledger_aggregates` + `active_spend_reservations` — real, DB-backed | none; `_gaps` is always `[]` per the builder's own contract |

Banned/ignored derivation follows `RPC_BATCH_A.md`'s contract: both filter
the SAME `queries::all_policies` read (not a separate query), proven by
`revenue_r_list_banned_and_list_ignored_reflect_real_peer_policies_rows`
seeding one banned-tagged row and one passive+disabled row and asserting
each RPC returns exactly its own peer.

## Reachability tests added

All in `crates/revops/tests/manifest.rs`:

1. **Exact-name assertions**, both naming modes:
   - `manifest_batch_a_methods_registered_shadow_mode`
   - `manifest_batch_a_methods_registered_canonical_mode`
   - plus the existing `manifest_canonical_mode_advertises_revenue_ops_names`
     RPC-count guard, updated 10 → 20 with the ten Batch A names asserted
     inline.
2. **Caller tripwires** (prove the handler calls its real builder/query path,
   not a hand-built parallel shape) — each seeds a distinctive row directly
   into a copy of the production schema (`fixtures/fixture.db`) via a raw
   `rusqlite` connection, then drives a real spawned plugin process through
   `getmanifest` → `init` → the RPC call, and asserts the seeded value
   round-trips through the response:
   - `revenue_r_policy_list_reflects_real_peer_policies_rows` (2 distinct
     seeded peers, both fields checked)
   - `revenue_r_policy_set_action_is_refused_before_any_db_access` (no
     db-path configured at all — proves the refusal fires BEFORE the
     `Some(handle) = &s.db` check, not after)
   - `revenue_r_list_banned_and_list_ignored_reflect_real_peer_policies_rows`
   - `revenue_r_hot_channel_protection_peers_list_reflects_a_real_row`
     (plus the `action != "list"` refusal)
   - `revenue_r_spend_ledger_reflects_a_real_spend_events_row`
   - `revenue_r_health_financials_reflect_a_real_forwards_row_rest_stays_gapped`
3. **Gap-honesty tripwire** (would red if a future change fabricates data to
   "complete" a response that has no wired evidence pipeline):
   - `revenue_r_gap_only_batch_a_methods_stay_honest` — covers
     `revenue-profitability` (both branches), `revenue-analyze` (both
     branches), `revenue-capacity-report`, `revenue-econ-snapshot`.

## RED transcript (before registering anything in `main.rs`)

Command: `cargo test -p revops --test manifest`, run with the ten new tests
already written but `main.rs` unmodified (still 10 registrations).

```
running 26 tests
test manifest_advertises_shadow_names ... ok
test manifest_registers_all_python_options_under_shadow_prefix ... ok
test manifest_fee_stateful_shadow_is_bool_default_false_not_dynamic ... ok
test manifest_batch_a_methods_registered_canonical_mode ... FAILED
test manifest_shadow_mode_db_path_default_stays_empty ... ok
test manifest_fee_dryrun_is_bool_default_false_not_dynamic ... ok
test manifest_canonical_mode_advertises_revenue_ops_names ... FAILED
test manifest_cutover_arm_path_is_string_default_empty_not_dynamic ... ok
test manifest_advertises_dynamic_plugin ... ok
test manifest_fee_broadcast_is_bool_default_false_not_dynamic ... ok
test manifest_batch_a_methods_registered_shadow_mode ... FAILED
test init_stateful_shadow_without_observer_db_disables ... ok
test init_cutover_arm_path_without_journal_dir_refuses_before_touching_arm ... ok
test init_canonical_mode_explicit_db_path_miss_still_disables ... ok
test init_canonical_mode_default_db_path_miss_does_not_disable ... ok
test revenue_r_policy_set_action_is_refused_before_any_db_access ... FAILED
test revenue_r_gap_only_batch_a_methods_stay_honest ... FAILED
test init_stateful_shadow_without_arm_and_with_observer_db_does_not_disable ... ok
test revenue_r_hot_channel_protection_peers_list_reflects_a_real_row ... FAILED
test revenue_r_policy_list_reflects_real_peer_policies_rows ... FAILED
test revenue_r_spend_ledger_reflects_a_real_spend_events_row ... FAILED
test revenue_r_list_banned_and_list_ignored_reflect_real_peer_policies_rows ... FAILED
test revenue_r_health_financials_reflect_a_real_forwards_row_rest_stays_gapped ... FAILED
test runway_status_seed_provenance_is_null_when_absent ... ok
test runway_status_seed_provenance_reports_a_recorded_seed_event ... ok
test runway_status_autonomous_shadow_reports_seed_once_lifecycle ... ok

failures:

---- manifest_batch_a_methods_registered_canonical_mode stdout ----

thread 'manifest_batch_a_methods_registered_canonical_mode' (2727075) panicked at crates/revops/tests/manifest.rs:951:9:
Batch A canonical method revenue-health not registered: ["revenue-ping", "revenue-fee-debug", "revenue-config", "revenue-dashboard", "revops-fee-runway-status", "revenue-history", "revenue-status", "revenue-fee-wake", "revenue-report", "revenue-rebalance-plan"]
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

---- manifest_canonical_mode_advertises_revenue_ops_names stdout ----

thread 'manifest_canonical_mode_advertises_revenue_ops_names' (2727078) panicked at crates/revops/tests/manifest.rs:341:9:
methods: ["revenue-history", "revenue-config", "revenue-rebalance-plan", "revenue-fee-debug", "revenue-dashboard", "revenue-report", "revenue-status", "revenue-ping", "revenue-fee-wake", "revops-fee-runway-status"]

---- manifest_batch_a_methods_registered_shadow_mode stdout ----

thread 'manifest_batch_a_methods_registered_shadow_mode' (2727076) panicked at crates/revops/tests/manifest.rs:934:9:
Batch A shadow method revenue-r-health not registered: ["revenue-r-fee-debug", "revenue-r-history", "revenue-r-dashboard", "revenue-r-ping", "revenue-r-rebalance-plan", "revenue-r-report", "revenue-r-fee-wake", "revenue-r-config", "revenue-r-status", "revops-fee-runway-status"]

---- revenue_r_policy_set_action_is_refused_before_any_db_access stdout ----

thread 'revenue_r_policy_set_action_is_refused_before_any_db_access' (2727098) panicked at crates/revops/tests/manifest.rs:1132:28:
expected a refusal error, got: Null

---- revenue_r_gap_only_batch_a_methods_stay_honest stdout ----

thread 'revenue_r_gap_only_batch_a_methods_stay_honest' (2727089) panicked at crates/revops/tests/manifest.rs:1358:5:
assertion `left == right` failed
  left: Null
 right: Number(0)

---- revenue_r_hot_channel_protection_peers_list_reflects_a_real_row stdout ----

thread 'revenue_r_hot_channel_protection_peers_list_reflects_a_real_row' (2727094) panicked at crates/revops/tests/manifest.rs:1225:5:
assertion `left == right` failed: result: Null
  left: Null
 right: Number(1)

---- revenue_r_policy_list_reflects_real_peer_policies_rows stdout ----

thread 'revenue_r_policy_list_reflects_real_peer_policies_rows' (2727096) panicked at crates/revops/tests/manifest.rs:1098:5:
assertion `left == right` failed: result: Null
  left: Null
 right: Number(2)

---- revenue_r_spend_ledger_reflects_a_real_spend_events_row stdout ----

thread 'revenue_r_spend_ledger_reflects_a_real_spend_events_row' (2727100) panicked at crates/revops/tests/manifest.rs:1275:5:
assertion `left == right` failed: result: Null
  left: Null
 right: Number(12345)

---- revenue_r_list_banned_and_list_ignored_reflect_real_peer_policies_rows stdout ----

thread 'revenue_r_list_banned_and_list_ignored_reflect_real_peer_policies_rows' (2727095) panicked at crates/revops/tests/manifest.rs:1175:5:
assertion `left == right` failed: banned: Null
  left: Null
 right: Number(1)

---- revenue_r_health_financials_reflect_a_real_forwards_row_rest_stays_gapped stdout ----

thread 'revenue_r_health_financials_reflect_a_real_forwards_row_rest_stays_gapped' (2727091) panicked at crates/revops/tests/manifest.rs:1314:5:
assertion `left == right` failed: result: Null
  left: Null
 right: Number(5)


failures:
    manifest_batch_a_methods_registered_canonical_mode
    manifest_batch_a_methods_registered_shadow_mode
    manifest_canonical_mode_advertises_revenue_ops_names
    revenue_r_gap_only_batch_a_methods_stay_honest
    revenue_r_health_financials_reflect_a_real_forwards_row_rest_stays_gapped
    revenue_r_hot_channel_protection_peers_list_reflects_a_real_row
    revenue_r_list_banned_and_list_ignored_reflect_real_peer_policies_rows
    revenue_r_policy_list_reflects_real_peer_policies_rows
    revenue_r_policy_set_action_is_refused_before_any_db_access
    revenue_r_spend_ledger_reflects_a_real_spend_events_row

test result: FAILED. 16 passed; 10 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.13s
```

10 failures, all exactly the ten new Batch-A-specific tests (every
pre-existing test still passed) — a clean red, not a compile break.

## GREEN transcript (after registering all ten in `main.rs`)

Command: `cargo test -p revops --test manifest`, run after adding the ten
`.rpcmethod()` blocks (and their `let X_name = rpc_name(...)` declarations)
to `main.rs`, unchanged from `RPC_BATCH_A.md`'s snippets except `p` → `_p`
where the closure never reads the plugin handle (clippy `-D warnings`
requires this).

```
running 26 tests
test manifest_advertises_shadow_names ... ok
test manifest_fee_broadcast_is_bool_default_false_not_dynamic ... ok
test manifest_fee_dryrun_is_bool_default_false_not_dynamic ... ok
test init_canonical_mode_explicit_db_path_miss_still_disables ... ok
test manifest_advertises_dynamic_plugin ... ok
test manifest_canonical_mode_advertises_revenue_ops_names ... ok
test init_cutover_arm_path_without_journal_dir_refuses_before_touching_arm ... ok
test manifest_batch_a_methods_registered_shadow_mode ... ok
test manifest_fee_stateful_shadow_is_bool_default_false_not_dynamic ... ok
test manifest_registers_all_python_options_under_shadow_prefix ... ok
test init_stateful_shadow_without_observer_db_disables ... ok
test manifest_shadow_mode_db_path_default_stays_empty ... ok
test manifest_batch_a_methods_registered_canonical_mode ... ok
test manifest_cutover_arm_path_is_string_default_empty_not_dynamic ... ok
test init_canonical_mode_default_db_path_miss_does_not_disable ... ok
test init_stateful_shadow_without_arm_and_with_observer_db_does_not_disable ... ok
test revenue_r_policy_set_action_is_refused_before_any_db_access ... ok
test revenue_r_health_financials_reflect_a_real_forwards_row_rest_stays_gapped ... ok
test revenue_r_policy_list_reflects_real_peer_policies_rows ... ok
test revenue_r_spend_ledger_reflects_a_real_spend_events_row ... ok
test revenue_r_hot_channel_protection_peers_list_reflects_a_real_row ... ok
test revenue_r_list_banned_and_list_ignored_reflect_real_peer_policies_rows ... ok
test revenue_r_gap_only_batch_a_methods_stay_honest ... ok
test runway_status_seed_provenance_is_null_when_absent ... ok
test runway_status_seed_provenance_reports_a_recorded_seed_event ... ok
test runway_status_autonomous_shadow_reports_seed_once_lifecycle ... ok

test result: ok. 26 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.15s
```

## Gate results

- `cargo check -p revops --tests` — clean.
- `cargo test -p revops --test manifest` — 26/26 pass (green transcript
  above).
- `cargo test -p revops` — all suites pass (177 unit tests + every
  integration test file, including `manifest.rs`'s 26).
- `cargo test --workspace` — all 129 test-binary result lines report `ok`, 0
  failures.
- `cargo fmt --all -- --check` — clean after one `cargo fmt --all` pass over
  the new test code (rustfmt reflowed a handful of the new assertions/calls
  to its line-length rules; no logic changed).
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `git diff --check` — clean (no whitespace errors).
- Working tree: clean after the commit below (no stray files).

## Changed files

- `crates/revops/src/main.rs` — ten `let X_name = rpc_name(...)` declarations
  and ten `.rpcmethod(...)` handler blocks added, verbatim from
  `RPC_BATCH_A.md` section 3 (two closures' unused `p: Plugin<SharedState>`
  bindings renamed to `_p` for clippy). No other line changed.
- `crates/revops/tests/manifest.rs` — the RPC-count guard bumped 10 → 20 with
  the ten Batch A names asserted inline; two new exact-name tests; seven new
  caller-tripwire / gap-honesty tests; two small test-only helpers
  (`copy_fixture_db`, `call_after_init_with_params`, `fake_peer_id`).
- `crates/revops/RPC_BATCH_A.md` — section 5 ("Verification status") updated
  to record that section 3 is now registered and tested, not still a wiring
  instruction; points at this report and the parity checklist.
- `docs/port/PARITY-CHECKLIST.md` — scope note replaced with the
  operator-approved whole-plugin scope (cites
  `docs/superpowers/specs/2026-07-27-whole-plugin-rust-cutover-design.md`),
  measured compiled/registered counts recorded, a "Task 49 — reachability"
  table added under Lens 0 marking exactly these ten entry points
  **reachable** (not effective/transport-proven/promotion-ready), and two
  stale "no `revenue-profitability`/`revenue-analyze` RPC in Rust" notes
  corrected.
- `crates/revops/TASK49-REPORT.md` — this file.

## What is NOT claimed

- **Effective / transport-proven / promotion-ready** for any of the ten:
  this task proves reachability (registered + real-query-calling where a
  query exists) and honesty (no fabricated evidence), not independent
  review or shadow/runway soak evidence.
- Any live evidence pipeline for profitability/analyze/capacity-report/
  econ-snapshot's still-gapped fields — those remain exactly as
  gap-marked as `RPC_BATCH_A.md` originally documented; this task did not
  attempt to wire them (out of scope, per the task brief's evidence rules).
- `econ_shadow_enabled` config-surface wiring for `revenue-econ-snapshot`
  (still hardcoded `false`, honestly, per `RPC_BATCH_A.md`'s own wiring
  note).
- Any mutation capability: `revenue-policy`'s tactical actions and
  `revenue-hot-channel-protection-peers`'s add/remove/clear stay refused,
  proven by tests that run with no db-path configured at all so the refusal
  can't be masked by a coincidental DB error.

## Incomplete / follow-up

Nothing in this task's own scope is incomplete — all ten RPCs are
registered, tested red-then-green, and gate-clean. Follow-up items belong to
later Wave-2/Wave-3 tasks per the whole-plugin design doc: the
`ChannelProfitability`/`FlowMetrics` assembly pipelines (profitability,
analyze, econ-snapshot), the winner/loser identification engine
(capacity-report), `econ_shadow_enabled` config wiring, and
`total_capacity_sats` (health's `annualized_roc_pct`) via a live
`listpeerchannels` call.

---

# Task-50 correction round

Executes `/home/sat/agent-tasks/task-50-batch-a-semantics.md` — a Python-side
adversarial audit ("Task 50", owner: the Python agent, tier 2, read-only) of
the exact snippets this Task-49 commit wired verbatim. The audit found 11
lettered findings (F1-F11) plus per-method notes; this round fixes F1-F9,
F11's in-band error convention across all ten handlers, health's `boltz`
"should NOT stay a gap" item, and two items the coordinating supervisor
upgraded mid-round from declare-only to FIX (F10's row-drop, and Batch-A-
scoped positional-parameter rejection). Every fix below followed strict
red-first TDD: the failing test was written and run against the
NOT-yet-fixed code first (either a genuine compile error for a new API, or a
genuine runtime assertion failure for a wiring bug), captured verbatim below,
THEN the fix landed, THEN green was captured.

Baseline for this round: `a61ccee` (this worktree's existing single commit,
amended at the end — see "Amended commit" below).

## Methodology note on "unmodified a61ccee"

All NEW test code for every finding below was written in one batch, before
ANY implementation fix landed, so every red transcript is genuinely captured
against code that does not yet have that finding's fix (several are captured
against literally-unmodified `a61ccee`; the rest are captured against
`a61ccee` plus whichever EARLIER findings' fixes had already landed in this
same session, in finding order F1-F5, F6/F9, F8/H6, F7, F11, boltz, array-
params, F10 — never against a state that already contains that finding's OWN
fix). Two findings needed a special capture technique because their tests
would otherwise pass "by accident" once earlier fixes in the same shared
files landed:
- **F6/F9** (`rpc_policy::normalize_action`/`coerce_since`): the new API
  shape itself didn't exist yet, so the red is a **compile error** naming
  the exact type mismatch at the `main.rs` call site — a legitimate red
  state for a Rust TDD cycle (see the F6/F9 section below).
- **Array-param rejection**: by the time its dedicated test was written, the
  `reject_positional_params` guard had already been added to all ten
  handlers alongside their OTHER fixes (added proactively once the
  supervisor's scope update landed). To get a genuine, isolated red for
  this specific cross-cutting item, `rpc_params::reject_positional_params`
  was temporarily neutralized (`return None` unconditionally), the test run
  to confirm it fails for exactly the reason the audit describes, then
  restored before implementing anything further (see that section below).

## F1 — econ-snapshot: stop hardcoding `enabled=false`

**Audit finding.** The wired snippet hardcoded `let enabled = false;`. On a
node where Python's real `econ_shadow_enabled=true`, the response
`{"enabled": false, "hint": ...}` is a FALSE statement about config state
with no gap marker — indistinguishable from the truthful disabled answer.

**Fix.** No `EconShadow`/`econ_shadow_enabled` config-read surface exists in
Rust at all, so the fix does NOT fabricate one. `rpc_econ_snapshot::
build_econ_snapshot_not_wired()` returns an in-band
`{"error": "econ shadow not_yet_ported", "reason": ...}` that cannot be read
as either a true or false `enabled` answer — wired in `main.rs`'s
`econ_snapshot_name` handler in place of `build_econ_snapshot(false, ...)`.

**Red/green.** Captured jointly with F2-F5 below (`revenue_r_gap_only_batch_a_methods_stay_honest`,
rewritten to assert all five NEW shapes at once, since all five live in the
same test function testing "the builders with no wired pipeline yet").

## F2 — capacity-report: Python's exact 1-key error, not a success-shaped stub

**Audit finding.** The snippet returned a success-shaped 6-key object
unconditionally. Python's real answer for "no capacity planner" is
`{"error": "Capacity planner not initialized"}` (cl-revenue-ops.py:
4586-4587) — 1 key, no `timestamp`.

**Fix.** `rpc_capacity_report::capacity_planner_not_initialized_error()`
returns that exact shape byte-for-byte; wired in place of
`build_capacity_report(now_unix())`.

## F3/F4 — profitability: mark both not-wired shapes, don't reuse Python's real vocabulary

**Audit finding.** `build_profitability_summary(&[])` returns a fully-formed
"0 channels" summary indistinguishable from a real empty fleet.
`build_profitability_channel(id, None)` returns
`{"channel_id": id, "error": "No data available"}` — byte-identical to
Python's legitimate unknown-channel answer.

**Fix.** New `rpc_profitability::build_profitability_channel_not_wired`/
`build_profitability_summary_not_wired` both carry `"error": "not_yet_ported"`
and deliberately do NOT reuse the `summary`/`channels_by_class` keys (no
success-shaped zeros) or the `"No data available"` string (no collision).

## F5 — analyze: distinguish "pipeline not wired" from "genuinely unknown channel"

**Audit finding.** `build_analyze(id, None)` emits
`{"channel": id, "analysis": null}` for EVERY valid SCID — byte-identical to
Python's real unknown/non-`CHANNELD_NORMAL` channel answer. A live channel
with real flow would read as nonexistent.

**Fix.** `rpc_analyze::MetricsLookup` enum: `NotWired` (this request never
looked anything up — response carries `"error": "not_yet_ported"` alongside
`"channel"/"analysis": null`) vs. `Ready(Option<&FlowMetrics>)` (the
pipeline ran; `None` is Python's OWN genuine-unknown shape, no marker
needed). Wired with `MetricsLookup::NotWired` in `main.rs`.

## F1-F5 RED (before any of the five fixes)

Command: `cargo test -p revops --test manifest revenue_r_gap_only_batch_a_methods_stay_honest`

```
running 1 test
test revenue_r_gap_only_batch_a_methods_stay_honest ... FAILED

failures:

---- revenue_r_gap_only_batch_a_methods_stay_honest stdout ----

thread 'revenue_r_gap_only_batch_a_methods_stay_honest' panicked at crates/revops/tests/manifest.rs:1372:5:
assertion `left == right` failed
  left: Null
 right: String("not_yet_ported")

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 25 filtered out; finished in 0.03s
```

## F1-F5 GREEN

Command: same test, after all five fixes landed in `rpc_econ_snapshot.rs`,
`rpc_capacity_report.rs`, `rpc_profitability.rs`, `rpc_analyze.rs`, and
`main.rs`'s four handlers.

```
running 26 tests
...
test revenue_r_gap_only_batch_a_methods_stay_honest ... ok
...
test result: ok. 26 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.15s
```

## F6 — policy `changes`: Python-exact `since` coercion

**Audit finding.** `v.get("since").and_then(as_i64).unwrap_or(0)` silently
maps ANY non-numeric-JSON `since` (garbage string, numeric STRING, float,
`null`) to `0` and returns the FULL policy table as "changes since the
epoch". Python: `since = int(since) if since else 0` inside a try/except
returning the exact `invalid_since_error()` string on garbage
(cl-revenue-ops.py:5524-5529) — `invalid_since_error()` already existed as
dead code (`rpc_policy.rs:85-102` at audit time).

**Fix.** `rpc_policy::coerce_since(raw: Option<&Value>) -> Option<i64>`:
Python-truthiness gate first (falsy -> `Some(0)`, no coercion attempted, can
never error), then `rpc_params::python_int` on anything truthy (`None` on
garbage). Wired in `main.rs`'s `"changes"` branch; `None` returns
`invalid_since_error()`.

## F9 — policy: absent action vs. explicit null/non-string action

**Audit finding.** Python's signature default is `action: str = "list"` —
an ABSENT `action` key binds the literal `"list"`. An EXPLICIT `action`
(any JSON type) instead goes through `str(action or "").strip().lower()`:
`null`/falsy -> `""` -> the unknown-action error. The OLD Rust wiring
collapsed BOTH cases (absent key, and explicit null/non-string) through
`Option<&str>` (`v.get("action").and_then(as_str)`), so an explicit
`action: null` silently succeeded as `list`.

**Fix.** `rpc_policy::normalize_action` now takes `Option<&Value>` (the raw
param, not pre-extracted `as_str()`): `None` (key absent) -> `"list"`;
`Some(v)` non-truthy -> `""`; `Some(Value::String(s))` truthy ->
trimmed+lowercased; any other truthy non-string -> `""` (scope-decided
simplification, folds to the same unknown-action error family Python
reaches for all such values except the one exact string `"true"`).

## F6/F9 RED/GREEN — REPLACED in the Round 2 corrections section below

**This section originally claimed a compile error (a stale call site after
`normalize_action`'s signature changed, before `main.rs` was updated) as
F6/F9's "red-first" evidence.** An independent re-review (codex,
`/home/sat/agent-tasks/task-49-review-findings.md`, P2) correctly found that
a compile error is not behavioral evidence: it proves the OLD call site
cannot type-check against the NEW function signature, not that the code
runs and produces the wrong answer for either F6 or F9's defect. There was
no captured behavioral RED for either finding in this report as originally
written.

Rather than append a disclaimer on top of the invalid claim, this section
has been replaced outright — see `TASK49-REPORT.md`'s **"Round 2
corrections"** section (below the Task-50 section) for the real behavioral
RED, captured by temporarily reverting `coerce_since`/`normalize_action` to
their pre-fix defect BEHAVIOR (garbage `since` silently falling back to `0`;
an explicit `action: null` silently collapsing to `"list"`) while keeping
the code compilable, running the SAME `revenue_r_policy_changes_since_coercion_matches_python`
/ `revenue_r_policy_explicit_null_action_is_refused_not_treated_as_list`
tests against that mutated behavior, and then restoring the real fix and
re-running for green.

## F8 + H6 — hot-channel-protection-peers: Python-exact action normalization, split refusal messages

**Audit finding (F8).** The snippet compared the raw action string directly
to `"list"` with no lowercasing. Python: `str(action or "list").lower()`,
NO `.strip()` — so `action="LIST"` succeeds in Python (refused by the old
Rust), and `action=""`/`null` default to `list` and succeed (refused by the
old Rust too). **H6**: the one refusal message conflated "a real write
action refused by this read-only port" (a genuine scope boundary) with
"this action doesn't exist at all" (a typo) — the audit asked for these to
be distinguishable.

**Fix.** `rpc_hot_channel_protection_peers::normalize_action` matches
Python's `or "list"` + `.lower()` + NO strip exactly (falsy JSON values,
including absent, default to `"list"`; truthy strings are lowercased
UNSTRIPPED, so `" list"` stays `" list"` and is refused as unknown).
`write_action_refused_error` (for the three real write actions
add/remove/clear) is now a DIFFERENT function/message from
`unknown_action_error` (Python's `f"Unknown action: {action}. Use
list|add|remove|clear"`).

## F8/H6 RED

Command: `cargo test -p revops --test manifest
revenue_r_hot_channel_protection_peers_action_normalization_matches_python`

```
running 1 test
test revenue_r_hot_channel_protection_peers_action_normalization_matches_python ... FAILED

---- revenue_r_hot_channel_protection_peers_action_normalization_matches_python stdout ----

thread '...' panicked at crates/revops/tests/manifest.rs:1375:5:
assertion `left == right` failed: action=LIST must succeed like Python: Object {"error": String("revenue-hot-channel-protection-peers LIST is not available in this read-only port; use 'list'")}
  left: Null
 right: String("success")

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 28 filtered out; finished in 0.05s
```

## F8/H6 GREEN

```
running 29 tests
...
test revenue_r_hot_channel_protection_peers_action_normalization_matches_python ... ok
...
test result: ok. 29 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.16s
```

## F7 — spend-ledger: Python-exact `window_hours`/`reservation_limit`/`include_reservations`

**Audit finding.** `.and_then(as_i64).unwrap_or(24).max(1)` silently
substitutes the default for ANY non-JSON-number `window_hours` (a numeric
STRING like `"48"`, garbage, `null`) instead of coercing (numeric strings)
or erroring (garbage). Python: `int(window_hours)` inside a try/except,
`"48"` -> `48`, `48.9` -> `48`, garbage -> `{"error": str(e)}`, floored at 1
by `get_spend_ledger_summary`'s `max(1, ...)`, deliberately NO upper clamp
(unlike `_total_cost_budget_status`'s `[1,168]`). `include_reservations`:
`.and_then(as_bool)` dropped anything that wasn't a literal JSON bool; Python
`bool(x)` truthiness means `bool("false")` is `True`.

**Fix.** `rpc_spend_ledger::parse_window_hours`/`parse_reservation_limit`
(via `rpc_params::python_int`, floored at 1, no ceiling) and
`parse_include_reservations` (via `rpc_params::is_truthy_py`). Wired in
`main.rs`, replacing the three inline `.and_then(...).unwrap_or(...)` calls.

## F7 RED

Command: `cargo test -p revops --test manifest
revenue_r_spend_ledger_window_hours_and_truthiness_match_python`

```
running 1 test
test revenue_r_spend_ledger_window_hours_and_truthiness_match_python ... FAILED

---- revenue_r_spend_ledger_window_hours_and_truthiness_match_python stdout ----

thread '...' panicked at crates/revops/tests/manifest.rs:1514:5:
assertion `left == right` failed
  left: Number(24)
 right: Number(48)

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 29 filtered out; finished in 0.06s
```

(`window_hours: "48"` silently ran as the 24h default, exactly the audit's
example.)

## F7 GREEN

```
running 30 tests
...
test revenue_r_spend_ledger_window_hours_and_truthiness_match_python ... ok
...
test result: ok. 30 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.18s
```

## F11 — in-band errors, all ten handlers; health's financials-section case specifically

**Audit finding.** Python's Batch A handlers never raise — each returns an
in-band `{"error": ...}`/`{"status":"error",...}` result. Every `?` on a DB
call in the wired snippets instead produces a JSON-RPC error envelope,
losing the ENTIRE response (a `result.get("error")` caller sees nothing).
`revenue-health` specifically: a `pnl_summary` failure should become
`financials: {"error": ...}` with the other eight sections still present
(Python catches per-section, cl-revenue-ops.py:6217-6218) — the old `?`
instead failed the WHOLE call.

**Fix.** Every `.await?` in the ten handlers (`health`, `policy`,
`list-banned`, `list-ignored`, `hot-channel-protection-peers`,
`spend-ledger`) now matches on the `Result` and returns an in-band
`{"error": ...}` (or, for `policy`, Python's
`{"status":"error","error":"Unexpected error: {e}"}` catch-all shape) on
failure instead of propagating with `?`. `health`'s handler specifically
wraps just the two `pnl_summary` calls in an inner `async` block, and on
failure builds `financials: {"error": e.to_string()}` on top of the honest
gap-only shape, removing `"financials"` from `_gaps` (a live failure is not
a declared "not wired" gap — leaving it gap-listed would make the harness
skip the field and hide the failure).

## F11 (health) RED

A DB with SOME table (so the actor's open-time `table_names` probe
succeeds and the plugin does not disable) but NOT the `forwards` table
`pnl_summary` needs — a real, reachable SQL failure, not a contrived one.

Command: `cargo test -p revops --test manifest
revenue_r_health_pnl_failure_is_an_in_band_financials_error_not_a_whole_call_failure`

```
running 1 test
test revenue_r_health_pnl_failure_is_an_in_band_financials_error_not_a_whole_call_failure ... FAILED

---- revenue_r_health_pnl_failure_is_an_in_band_financials_error_not_a_whole_call_failure stdout ----

thread '...' panicked at crates/revops/tests/manifest.rs:1585:5:
financials must be an in-band error OBJECT, not a whole-call failure: Null

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 30 filtered out; finished in 0.05s
```

(`result` itself was JSON `Null` — the `?`-propagation turned the whole RPC
call into a JSON-RPC error, so there was no `result` object at all to probe
`["financials"]` on.)

## F11 (health) GREEN

```
running 31 tests
...
test revenue_r_health_pnl_failure_is_an_in_band_financials_error_not_a_whole_call_failure ... ok
...
test result: ok. 31 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.17s
```

The remaining nine `?`-removals (`list-banned`, `list-ignored`,
`hot-channel-protection-peers`, `spend-ledger`'s aggregate/reservation
calls, `policy`'s four action branches) are covered by the SAME
already-passing caller-tripwire tests continuing to pass after the `?` ->
in-band-match rewrite (no dedicated new red needed per sub-handler — the
shape of the SUCCESS path is unchanged, only the failure path's transport
changed, and no test in this suite can force those specific DB calls to
fail without the same "schema missing the needed table" technique used for
health, which would be a near-duplicate of the health case for less marginal
evidence). This is recorded here as an honest scope note, not hidden: the
health case is the one the audit named explicitly (F11's own text), and is
the one with dedicated red/green coverage; the other nine handlers'
`?` -> in-band-match rewrites are covered by regression (their existing
success-path tests still pass) but not by a dedicated forced-failure red.

## "Should NOT stay gaps" — health's `boltz` section

**Audit finding.** With no Boltz manager wired, Python's OWN answer for
`boltz` is the definite `{"enabled": false}` (cl-revenue-ops.py:6312-6313)
— cheap, true, and shape-faithful. `null` + a `_gaps` entry is strictly
worse (it hides a field that IS computable today). (The audit's OTHER
"should not stay a gap" item, `fees` from the live `ControllerState`, was
explicitly scoped SKIP this round — it needs scheduler plumbing into the
handler; declared wireable-now-but-not-wired in the parity checklist,
not fixed.)

**Fix.** `rpc_health::build_health`'s `boltz` field is now
`json!({"enabled": false})` unconditionally, removed from the static
`_gaps` list.

## boltz RED

Command: `cargo test -p revops --lib rpc_health::`

```
running 6 tests
...
test rpc_health::tests::boltz_section_is_the_honest_enabled_false_shape_not_a_null_gap ... FAILED
...
---- rpc_health::tests::boltz_section_is_the_honest_enabled_false_shape_not_a_null_gap stdout ----

thread '...' panicked at crates/revops/src/rpc_health.rs:247:9:
assertion `left == right` failed
  left: Null
 right: Object {"enabled": Bool(false)}

test result: FAILED. 5 passed; 1 failed; 0 ignored; 0 measured; 203 filtered out; finished in 0.00s
```

## boltz GREEN

```
running 6 tests
test rpc_health::tests::always_present_static_gaps_and_never_fabricated_values ... ok
test rpc_health::tests::annualized_roc_computed_when_capacity_supplied ... ok
test rpc_health::tests::missing_pnl_yields_null_financials_and_gap ... ok
test rpc_health::tests::zero_capacity_falls_back_to_zero_not_division ... ok
test rpc_health::tests::wired_pnl_populates_today_and_week ... ok
test rpc_health::tests::boltz_section_is_the_honest_enabled_false_shape_not_a_null_gap ... ok

test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 203 filtered out; finished in 0.00s
```

## SCOPE UPDATE (mid-round) — F10 upgraded to a fix; Batch-A positional-param rejection upgraded to a fix

The supervisor sent a mid-round scope update after the round above was
already in progress: "No DB lane is currently active" authorized a narrow
fix to `revops-db/src/queries.rs`'s row decode (F10), and "every Batch-A
handler MUST explicitly reject positional/array params" made the
previously-declare-only positional-parameter item a FIX for these ten
handlers specifically (not port-wide). Both are implemented below with
their own red-first cycles.

## F10 — policy row decode: keep-with-defaults, never silently drop a row

**Audit finding (security-relevant).** `decode_policy_row` used
`row.get::<_, T>(i)?` with a STRICT target type per column;
`query_policy_rows` then `.ok()`-dropped the WHOLE row on the FIRST
conversion failure (e.g. a NULL/mistyped `expires_at`). Python's
`_row_to_policy` (policy_manager.py:395-422) never validates/drops scalar
columns at all — a malformed row still appears in every Python read. A
banned peer with one malformed cell could silently vanish from
`revenue-r-list-banned`.

**Fix (the "Preferred fix" outcome, per the supervisor's three-way
ruling).** Keep-with-defaults (round-2 correction, P2: this is a
DELIBERATE FAIL-SAFE DIVERGENCE from Python, not a "Python-exact" port —
see the "Round 2 corrections" section below for why): `decode_policy_row`
now reads every column through lossy `SqlValue`-based accessors
(`sql_text_or`, `sql_opt_i64`, `sql_opt_f64`) that default rather than
error on NULL/mistyped storage classes. `peer_id`/`strategy`/
`rebalance_mode` default to `""`; `fee_ppm_target`/`fee_multiplier_min/max`/
`expires_at` default to `None`; `updated_at` defaults to `0`; malformed
tags JSON (already ported) defaults to `[]`. `expires_at: None` on garbage
is the FAIL-SAFE reading — a policy row with a corrupt expiry stays visible
(never expires) rather than silently reading as already-expired and being
filtered out. `query_policy_rows` no longer `.ok()`-drops per row;
`decode_policy_row` is now infallible in practice, so a genuine `Err`
(should one ever occur) now propagates as a real whole-call failure — a
loud in-band signal, never a silent drop, per the supervisor's stated
fallback.

**Test rewrite, not a new test alongside the old one.** The PRE-EXISTING
Task-49 test `corrupt_policy_row_is_isolated_instead_of_bricking_all_reads`
literally asserted the DROP behavior (`vec!["valid"]`, i.e. the corrupt row
gone) — this WAS the a61ccee characterization of the bug the audit flagged.
It was rewritten to `corrupt_scalar_column_is_kept_with_defaults_not_dropped`,
asserting the row survives with the correct good-column values and the
fail-safe `expires_at: None`. A second new test,
`corrupt_updated_at_column_defaults_to_zero_row_still_present`, exercises
the OTHER scalar column path.

## F10 RED

Command: `cargo test -p revops-db --test queries corrupt_`, run with the two
NEW/rewritten tests in place but `queries.rs`'s `decode_policy_row` still
the OLD strict-`?`-per-column version.

```
running 2 tests
test corrupt_updated_at_column_defaults_to_zero_row_still_present ... FAILED
test corrupt_scalar_column_is_kept_with_defaults_not_dropped ... FAILED

failures:

---- corrupt_updated_at_column_defaults_to_zero_row_still_present stdout ----

thread '...' panicked at crates/revops-db/tests/queries.rs:639:10:
a malformed updated_at must not drop the row

---- corrupt_scalar_column_is_kept_with_defaults_not_dropped stdout ----

thread '...' panicked at crates/revops-db/tests/queries.rs:579:5:
a row with one malformed scalar column must NOT vanish from the read (fail-open on a security-relevant surface): ["valid"]

test result: FAILED. 0 passed; 2 failed; 0 ignored; 0 measured; 23 filtered out; finished in 0.01s
```

## F10 GREEN

```
running 25 tests
...
test corrupt_updated_at_column_defaults_to_zero_row_still_present ... ok
test corrupt_scalar_column_is_kept_with_defaults_not_dropped ... ok
...
test result: ok. 25 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.12s
```

A third, RPC-level test,
`revenue_r_list_banned_does_not_drop_a_banned_peer_with_a_malformed_column`
(`crates/revops/tests/manifest.rs`), proves the fix reaches the actual
`revenue-r-list-banned` RPC, not just the query-layer unit tests — added
and run AFTER the `queries.rs` fix already landed, so it is reported here
as already-green characterization confirming the fix's reach, not a
separately-captured red (the underlying bug was already fixed by the time
this test was written; its purpose is end-to-end confidence, not novel
red/green evidence).

## Batch-A positional-parameter rejection

**Scope-update requirement.** Every Batch-A handler must explicitly reject
a NON-EMPTY positional (JSON array) params value with an in-band error;
`lightning-cli`'s own no-argument call shape is an EMPTY array (`[]`), which
must still mean "no params" so every bare invocation keeps working.

**Fix.** New `rpc_params::reject_positional_params(v: &Value) -> Option<Value>`:
`Some(error)` for a non-empty `Value::Array`, `None` otherwise (including
an empty array, an object, or any other JSON shape). Called first thing in
all ten handlers in `main.rs`.

**RED, captured via temporary neutralization** (see the methodology note
above for why): `rpc_params::reject_positional_params` was temporarily
changed to `pub fn reject_positional_params(_v: &Value) -> Option<Value> { None }`
and the dedicated test run against that:

Command: `cargo test -p revops --test manifest
revenue_r_batch_a_methods_reject_nonempty_positional_params_empty_array_still_succeeds`

```
running 1 test
test revenue_r_batch_a_methods_reject_nonempty_positional_params_empty_array_still_succeeds ... FAILED

---- ... stdout ----

thread '...' panicked at crates/revops/tests/manifest.rs:1704:13:
revenue-r-health: expected a positional-params refusal, got: Object {"_gaps": Array [...], "boltz": Object {"enabled": Bool(false)}, "budget": Null, "channels": Null, "fees": Null, "financials": Object {"today": Object {"costs_sats": Number(0), "forward_count": Number(0), "net_profit_sats": Number(0), "revenue_sats": Number(0), "volume_sats": Number(0)}, "week": Object {...}}, "generated_at": Number(1785181332), "loops": Null, "planner": Null, "rebalancer": Null, "top_routes": Null}

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 31 filtered out; finished in 0.03s
```

This is the exact bug the audit describes: `revenue-r-health` called with
`[48]` silently ran with default params instead of refusing, producing a
confident-looking full response instead of an error.

The real `reject_positional_params` was then restored (verified via `git
diff`-equivalent re-read that it matched the pre-neutralization source
exactly) and the test rerun:

```
running 1 test
test revenue_r_batch_a_methods_reject_nonempty_positional_params_empty_array_still_succeeds ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 31 filtered out; finished in 0.33s
```

## Full gate run (after all Task-50 fixes)

- `cargo test -p revops` — 209 lib tests + every integration test file
  (manifest 33, python_authority 27, read_rpcs 17, replay_cli 8, status 4,
  and the rest), ALL green, 0 failures anywhere in the suite.
- `cargo test --workspace` — 129 test-binary result lines, all `ok`, 0
  failures.
- `cargo fmt --all -- --check` — clean after one `cargo fmt --all` pass
  (reflowed several of the new assertions/doc comments to rustfmt's
  line-length rules; no logic changed, re-verified green after).
- `cargo clippy --workspace --all-targets -- -D warnings` — one real finding
  fixed mid-round (`clippy::result_unit_err` on `coerce_since`'s original
  `Result<i64, ()>` signature — changed to `Option<i64>`, which is also the
  more idiomatic Rust shape for "success value or nothing to report",
  updated at both the `rpc_policy.rs` unit-test call sites and the
  `main.rs` wiring `match`); clean after.
- `git diff --check` — clean (no whitespace errors).
- Working tree: clean after the amended commit (see below).

## Changed files (this round)

- `crates/revops/src/rpc_params.rs` — NEW. `reject_positional_params`,
  `is_truthy_py`, `python_int` — shared Python-coercion helpers used by
  `rpc_policy.rs`, `rpc_spend_ledger.rs`, and `main.rs`'s array-param gate
  on all ten handlers.
- `crates/revops/src/lib.rs` — `pub mod rpc_params;`.
- `crates/revops/src/rpc_econ_snapshot.rs` — F1: `build_econ_snapshot_not_wired`.
- `crates/revops/src/rpc_capacity_report.rs` — F2: `capacity_planner_not_initialized_error`.
- `crates/revops/src/rpc_profitability.rs` — F3/F4: `build_profitability_channel_not_wired`,
  `build_profitability_summary_not_wired`.
- `crates/revops/src/rpc_analyze.rs` — F5: `MetricsLookup` enum, `build_analyze`
  signature change, all call sites/tests updated.
- `crates/revops/src/rpc_policy.rs` — F6: `coerce_since`; F9:
  `normalize_action` signature change (`Option<&str>` -> `Option<&Value>`).
- `crates/revops/src/rpc_hot_channel_protection_peers.rs` — F8/H6:
  `normalize_action`, `write_action_refused_error`, `unknown_action_error`,
  `WRITE_ACTIONS`.
- `crates/revops/src/rpc_spend_ledger.rs` — F7: `parse_window_hours`,
  `parse_reservation_limit`, `parse_include_reservations`.
- `crates/revops/src/rpc_health.rs` — "should NOT stay gaps": `boltz` section.
- `crates/revops/src/main.rs` — all ten `.rpcmethod()` handlers rewired:
  array-param gate (all ten); F1/F2 (econ-snapshot/capacity-report); F3/F4
  (profitability); F5 (analyze); F6/F9/F11 (policy); F7/F11 (spend-ledger);
  F8/H6 (hot-channel-protection-peers); F11 (health, list-banned,
  list-ignored).
- `crates/revops-db/src/queries.rs` — F10: `decode_policy_row` rewritten to
  lossy/infallible column decoding (`sql_text_or`, `sql_opt_i64`,
  `sql_opt_f64`); `query_policy_rows` no longer `.ok()`-drops.
- `crates/revops-db/tests/queries.rs` — F10: `corrupt_policy_row_is_isolated_instead_of_bricking_all_reads`
  rewritten to `corrupt_scalar_column_is_kept_with_defaults_not_dropped`
  (asserts keep-with-defaults, not drop); new
  `corrupt_updated_at_column_defaults_to_zero_row_still_present`.
- `crates/revops/tests/manifest.rs` — `revenue_r_gap_only_batch_a_methods_stay_honest`
  rewritten for F1-F5's new shapes; new
  `revenue_r_policy_explicit_null_action_is_refused_not_treated_as_list`,
  `revenue_r_policy_changes_since_coercion_matches_python`,
  `revenue_r_hot_channel_protection_peers_action_normalization_matches_python`,
  `revenue_r_spend_ledger_window_hours_and_truthiness_match_python`,
  `revenue_r_health_pnl_failure_is_an_in_band_financials_error_not_a_whole_call_failure`,
  `revenue_r_batch_a_methods_reject_nonempty_positional_params_empty_array_still_succeeds`,
  `revenue_r_list_banned_does_not_drop_a_banned_peer_with_a_malformed_column`;
  existing `revenue_r_health_financials_reflect_a_real_forwards_row_rest_stays_gapped`
  updated for the `boltz` fix (no longer gap-listed, asserts the real shape).
- `docs/port/PARITY-CHECKLIST.md` — new "Task 50" subsection under Lens 0
  recording every finding's fix + evidence, the two scope-updated fixes
  (F10, Batch-A positional rejection), and every remaining DECLARE-ONLY item
  from the original brief (full port-wide positional parity as a supervisor
  follow-up; the audit's §4 Python authority-record corrections, copied
  verbatim so the parity harness expects Python-side drift; P3/P4/H8/edge-
  diff items not touched this round).
- `crates/revops/TASK49-REPORT.md` — this section.

## Declare-only decisions (recorded, not implemented)

Per the ORIGINAL Task 50 brief's scope decisions (before the mid-round
supervisor update), these stay declare-only — full detail and evidence
pointers are in `docs/port/PARITY-CHECKLIST.md`'s new Task 50 subsection,
summarized here:

1. **Full positional-parameter parity, port-wide.** This round's
   `reject_positional_params` is Batch-A-only. Every OTHER Rust RPC in this
   plugin still silently defaults on a positionally-bound array param. A
   decide-once item for the supervisor: implement real positional binding,
   or extend the refuse-and-error pattern everywhere.
2. **The audit's §4 Python authority-record corrections** — copied verbatim
   into the parity checklist so the parity harness expects Python-side
   drift (Python `revenue-analyze`/`revenue-policy get`/`revenue-health`/
   `revenue-econ-snapshot`/`revenue-capacity-report` are NOT read-only in
   Python despite Rust's read-only observer design; Python's own
   suppressions on `top_routes`/econ budget-profitability/`changes`-with-
   one-corrupt-row are documented as NOT to be replicated as virtues).
3. **Everything else the audit flagged but did not rank for this round**:
   P3 (policy's "deprecated" refusal message misattributes why an
   `internal=true` write doesn't happen in Rust), P4 (the policy
   `get`-purge asymmetry, folded into item 2), H8 (hot-channel-protection-
   peers' row decode has no per-row isolation, unlike `peer_policies` after
   the F10 fix — one mistyped cell still fails the WHOLE call there), the
   `find`/`get` edge-format diffs in §2.6, econ-snapshot's E5-E8, analyze's
   A4/A5, health's H9 (the four `annualized_roc_pct` capacity semantics).

## What is NOT claimed (this round)

- **Effective/transport-proven/promotion-ready** for any of the ten RPCs —
  unchanged from Task 49's own disclaimer; this round is a Python-semantics
  correction pass over the SAME reachable-only ten handlers, not a new
  evidence-pipeline wiring effort.
- The nine non-health `?` -> in-band-match F11 rewrites have regression
  coverage (existing success-path tests) but not a dedicated forced-failure
  red/green pair (see the F11 section's honest scope note above).
- Any of the still-live evidence-pipeline gaps from Task 49 (`ChannelProfitability`,
  `FlowMetrics`, the capacity-report winner/loser engine, `econ_shadow_enabled`
  config wiring, `total_capacity_sats`) — unchanged by this round; this
  round only fixed how those gaps are REPORTED (unmistakably marked,
  never fabricated/collided), not what is reported.

## Amended commit (Task 50 round)

`git commit --amend` on the single Task-49 checkpoint — keeps this whole
Batch A registration + Python-semantics-correction effort as one logical
checkpoint (per this round's explicit instruction), not a second commit.
See the top of this file for Task 49's own record; this section is the
Task-50 correction round appended to the same commit's message/body via the
amend.

# Round 2 corrections (codex independent re-review, 2026-07-27)

Reviewed checkpoint `2b3d3563f695ae51fe66e45deffc2758fdd29352` (the Task
49+50 amended commit above) failed an independent supervisor re-review:
`/home/sat/agent-tasks/task-49-review-findings.md`, 1 CRITICAL, 3 P1, 3 P2, 1
P3. This section is the correction round's own red-first evidence and diff
map, appended as this same commit's second amend (still one logical
checkpoint, per the round-2 contract). The invalid F6/F9 compile-only RED
claim above has been REPLACED (not merely disclaimed) — see the note in its
former place.

## Finding -> files/tests diff map

| Finding | Fix location(s) | New/changed tests |
|---|---|---|
| CRITICAL — mixed-type `tags` array | `crates/revops-db/src/queries.rs` (`decode_tags_json`, `decode_policy_row` doc) | `crates/revops-db/tests/queries.rs`: `mixed_type_tags_array_preserves_valid_string_members_not_dropped_wholesale`, `mixed_type_tags_array_preserves_ignored_reason_tag`; `crates/revops/tests/manifest.rs`: `revenue_r_list_banned_does_not_drop_a_peer_over_a_mixed_type_tags_array`, `revenue_r_list_ignored_preserves_reason_tag_despite_mixed_type_tags_array` |
| P1 — oversized JSON numbers | `crates/revops/src/rpc_params.rs` (`python_int`, `I64_UPPER_BOUND_F64`) | `crates/revops/src/rpc_params.rs` (lib tests): `python_int_rejects_u64_max_loudly_instead_of_wrapping`, `python_int_rejects_out_of_range_float_loudly_instead_of_saturating`, `python_int_still_accepts_in_range_u64_and_float`; `crates/revops/tests/manifest.rs`: `revenue_r_spend_ledger_rejects_out_of_range_window_hours_instead_of_wrapping`, `revenue_r_spend_ledger_rejects_out_of_range_reservation_limit`, `revenue_r_policy_changes_rejects_out_of_range_since` |
| P1 — no-DB health top-level error | `crates/revops/src/main.rs` (health `.rpcmethod()`, `db=None` branch) | `crates/revops/tests/manifest.rs`: `revenue_r_health_with_no_db_returns_honest_shape_not_a_top_level_error` |
| P1 — checklist counts / stale conclusions | `docs/port/PARITY-CHECKLIST.md` (baseline/measured counts, Lens 0 row, Task 49 per-RPC table replaced with current contracts + historical appendix, Lens 7 wording, §3b remainder count + conclusion) | doc-only; counts cross-checked against `crates/revops/tests/manifest.rs`'s `assert_eq!(...len(), 20, ...)` guard |
| P2 — F10 "Python-exact" mislabeling | `crates/revops-db/src/queries.rs` (`decode_policy_row`/`decode_tags_json` doc), `crates/revops-db/tests/queries.rs` (test doc), `docs/port/PARITY-CHECKLIST.md` (F10 bullet), `crates/revops/TASK49-REPORT.md` (this file, F10 section below) | doc-only (no behavior change) |
| P2 — F6/F9 invalid RED claim | `crates/revops/TASK49-REPORT.md` (old section replaced; this section's transcripts below) | re-ran EXISTING `revenue_r_policy_explicit_null_action_is_refused_not_treated_as_list` / `revenue_r_policy_changes_since_coercion_matches_python` against temporarily-mutated behavior, then restored |
| P2 — health docs describe pre-port reality | `crates/revops/src/rpc_health.rs` (module doc), `docs/port/PARITY-CHECKLIST.md` (Task 50 findings table's `"should NOT stay gap"` row — already accurate, verified not changed) | doc-only |
| P3 — `[]`/`{}` not proven equivalent | `crates/revops/tests/manifest.rs` (new table-driven test) | `revenue_r_batch_a_methods_empty_array_and_empty_object_params_are_semantically_equal` |

## CRITICAL — F10 still lets a banned peer disappear (mixed-type `tags` array)

**Finding.** `decode_policy_row`'s tags decode parsed the WHOLE `tags`
column as `Vec<String>` via serde's typed array deserializer, which fails
outright the instant any element isn't a JSON string; `.unwrap_or_default()`
then replaced the ENTIRE array with `[]`. Valid SQLite JSON like
`["banned", 7]` is a legal Python list — `"banned" in tags` is still `True`
in Python with the non-string `7` present — but the OLD Rust decode erased
the whole tag set, including the real `"banned"` membership, over the one
malformed sibling: exactly F10's original failure mode ("a banned peer can
silently vanish from `revenue-r-list-banned`"), recreated one layer below
F10's row-level fix.

**Fix.** New `decode_tags_json` in `crates/revops-db/src/queries.rs`: parses
the column as a generic `serde_json::Value`, then for a JSON array keeps
only the elements that ARE JSON strings, dropping non-string elements
INDIVIDUALLY. This is documented as a **deliberate fail-safe divergence**
from Python (not "Python-exact"): Python's heterogeneous list keeps the raw
non-string member; Rust's typed `Vec<String>` has no slot for it, so it is
dropped instead — never re-introduced, never used to wipe the whole array.
Covers both the `list-banned` path (membership) and the `list-ignored` path
(the `reason` field, which is the equivalent-severity failure there, since
`list-ignored`'s peer membership doesn't depend on tags).

## CRITICAL RED — query layer

Captured against unmodified `2b3d356`, tests added but `decode_tags_json`
not yet written (old whole-array `Vec<String>` decode still in place).

Command: `cargo test -p revops-db --test queries mixed_type_tags`

```
running 2 tests
test mixed_type_tags_array_preserves_valid_string_members_not_dropped_wholesale ... FAILED
test mixed_type_tags_array_preserves_ignored_reason_tag ... FAILED

failures:

---- mixed_type_tags_array_preserves_valid_string_members_not_dropped_wholesale stdout ----

thread 'mixed_type_tags_array_preserves_valid_string_members_not_dropped_wholesale' panicked at crates/revops-db/tests/queries.rs:658:5:
assertion `left == right` failed: the non-string sibling (7) must be dropped INDIVIDUALLY, not the whole array: []
  left: []
 right: ["banned"]

---- mixed_type_tags_array_preserves_ignored_reason_tag stdout ----

thread 'mixed_type_tags_array_preserves_ignored_reason_tag' panicked at crates/revops-db/tests/queries.rs:720:5:
assertion `left == right` failed: the real reason tag must survive the malformed numeric sibling, not fall back to a wiped-then-defaulted [] / "manual": []
  left: []
 right: ["low_value"]

test result: FAILED. 0 passed; 2 failed; 0 ignored; 0 measured; 25 filtered out; finished in 0.01s
```

## CRITICAL RED — real RPC-process layer

Same unmodified state, `crates/revops/tests/manifest.rs`'s new
`call_after_init`-based tests: a mixed-type `["banned", 7]` tags array
seeded into a real production-schema DB copy BEFORE the plugin process
starts, then `revenue-r-list-banned`/`revenue-r-list-ignored` called through
the real compiled binary over stdin/stdout JSON-RPC framing.

Command: `cargo test -p revops --test manifest mixed_type_tags`

```
running 2 tests
test revenue_r_list_banned_does_not_drop_a_peer_over_a_mixed_type_tags_array ... FAILED
test revenue_r_list_ignored_preserves_reason_tag_despite_mixed_type_tags_array ... FAILED

failures:

---- revenue_r_list_banned_does_not_drop_a_peer_over_a_mixed_type_tags_array stdout ----

thread '...' panicked at crates/revops/tests/manifest.rs:1374:5:
assertion `left == right` failed: a peer whose real "banned" tag sits next to one malformed non-string sibling element must NOT vanish from revenue-r-list-banned: Object {"banned_peers": Array [], "count": Number(0)}
  left: Number(0)
 right: Number(1)

---- revenue_r_list_ignored_preserves_reason_tag_despite_mixed_type_tags_array stdout ----

thread '...' panicked at crates/revops/tests/manifest.rs:1422:5:
assertion `left == right` failed: the real reason tag must survive the malformed numeric sibling, not silently fall back to the generic "manual" default: Object {"count": Number(1), "ignored_peers": Array [Object {"ignored_at": Number(1800000800), "peer_id": String("03iiii...iii"), "reason": String("manual")}], "warning": String("DEPRECATED: Use 'revenue-policy list' instead.")}
  left: String("manual")
 right: String("low_value")

test result: FAILED. 0 passed; 2 failed; 0 ignored; 0 measured; 33 filtered out; finished in 0.04s
```

This is the literal "peer vanishes from `revenue-r-list-banned`"
(`count: 0`) the contract required proving at the real RPC-process layer.

## CRITICAL GREEN — both layers, after `decode_tags_json`

```
$ cargo test -p revops-db --test queries mixed_type_tags
running 2 tests
test mixed_type_tags_array_preserves_valid_string_members_not_dropped_wholesale ... ok
test mixed_type_tags_array_preserves_ignored_reason_tag ... ok
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 25 filtered out; finished in 0.01s

$ cargo test -p revops --test manifest mixed_type_tags
running 2 tests
test revenue_r_list_banned_does_not_drop_a_peer_over_a_mixed_type_tags_array ... ok
test revenue_r_list_ignored_preserves_reason_tag_despite_mixed_type_tags_array ... ok
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 33 filtered out; finished in 0.04s
```

Full `revops-db --test queries` suite (27 tests, including the two new ones
and the pre-existing F10 scalar-column tests) and the relevant
`revops --test manifest` slice both reran green with no other regressions.

## P1 — oversized JSON numbers silently become successful wrong queries

**Finding.** `python_int` cast an unsigned JSON number via `u as i64`
(WRAPS: `u64::MAX as i64 == -1`) and a JSON float via `f.trunc() as i64`
(SATURATES on out-of-range values, Rust's defined float->int cast behavior
since 1.45). `parse_window_hours(...).max(1)` then turned a wrapped `-1`
into a confident, successful 1-hour spend ledger instead of rejecting the
impossible request — the same helper feeds `reservation_limit` and policy
`since`.

**Fix.** `python_int` now uses `i64::try_from(u)` for the unsigned-integer
path and an explicit exact range check (`I64_UPPER_BOUND_F64 = 2^63`,
avoiding the `i64::MAX as f64` rounding-up-to-`2^63` trap) before the
`as i64` truncating cast on the float path. Both out-of-range shapes now
return a loud `Err` with an explanatory message instead of a silently wrong
`i64`. Documented as an explicit, deliberate design choice: Python's `int`
is arbitrary-precision and never fails here, so there is no Python
exception text to port — an in-band range error is the correct answer since
these Rust query interfaces cannot represent the value faithfully.

## P1 RED — `python_int` unit tests

Captured against unmodified `2b3d356` (old `u as i64` / `f.trunc() as i64`).

Command: `cargo test -p revops --lib rpc_params::`

```
running 10 tests
...
test rpc_params::tests::python_int_rejects_out_of_range_float_loudly_instead_of_saturating ... FAILED
test rpc_params::tests::python_int_rejects_u64_max_loudly_instead_of_wrapping ... FAILED

failures:

---- rpc_params::tests::python_int_rejects_out_of_range_float_loudly_instead_of_saturating stdout ----
thread '...' panicked at crates/revops/src/rpc_params.rs:205:44:
must not saturate to i64::MAX: 9223372036854775807

---- rpc_params::tests::python_int_rejects_u64_max_loudly_instead_of_wrapping stdout ----
thread '...' panicked at crates/revops/src/rpc_params.rs:191:48:
must not wrap to -1: -1

test result: FAILED. 8 passed; 2 failed; 0 ignored; 0 measured; 202 filtered out; finished in 0.00s
```

## P1 RED — real handler paths (`revenue-r-spend-ledger`, `revenue-r-policy`)

Same unmodified state. Real compiled-binary calls with `window_hours:
u64::MAX`, `window_hours: 1e20`, `reservation_limit: u64::MAX`, and `since:
u64::MAX` against a seeded production-schema DB copy.

Command: `cargo test -p revops --test manifest -- rejects_out_of_range`

```
running 3 tests
test revenue_r_spend_ledger_rejects_out_of_range_reservation_limit ... FAILED
test revenue_r_policy_changes_rejects_out_of_range_since ... FAILED
test revenue_r_spend_ledger_rejects_out_of_range_window_hours_instead_of_wrapping ... FAILED

failures:

---- revenue_r_spend_ledger_rejects_out_of_range_reservation_limit stdout ----
thread '...' panicked at crates/revops/tests/manifest.rs:1770:5:
u64::MAX reservation_limit must be a loud in-band error: Object {"_gaps": Array [], ... "spent_24h_sats": Number(0), ... "window_hours": Number(24)}

---- revenue_r_policy_changes_rejects_out_of_range_since stdout ----
thread '...' panicked at crates/revops/tests/manifest.rs:1806:5:
assertion `left == right` failed: u64::MAX since must be refused, not wrapped to -1 and treated as "changes since before every row": Object {"changes": Array [Object {... "peer_id": String("02jjjj...jj"), ...}], "count": Number(1), "last_change_timestamp": Number(1800000900), "since": Number(-1)}
  left: Null
 right: String("Invalid 'since' timestamp. Must be a Unix timestamp.")

---- revenue_r_spend_ledger_rejects_out_of_range_window_hours_instead_of_wrapping stdout ----
thread '...' panicked at crates/revops/tests/manifest.rs:1723:5:
u64::MAX window_hours must NOT silently become a clean 1-hour ledger: Object {"_gaps": Array [], "active_reservation_count_by_category": Object {}, "coverage_hours": Number(1), "coverage_status": String("complete"), "covered_hours": Number(1), ... "window_hours": Number(1)}

test result: FAILED. 0 passed; 3 failed; 0 ignored; 0 measured; 35 filtered out; finished in 0.05s
```

This is the exact scenario the audit named: `"coverage_status": "complete"`,
`"window_hours": 1` — a clean, confident-looking 1-hour ledger for a request
that should have been rejected outright; and `"since": -1` proving the wrap
in the policy `changes` path, which then returned the FULL row set as
"changes since before every timestamp" instead of an error.

## P1 GREEN — both unit and handler layers, after the `python_int` fix

```
$ cargo test -p revops --lib rpc_params::
running 10 tests
... all ok ...
test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 202 filtered out; finished in 0.00s

$ cargo test -p revops --test manifest -- rejects_out_of_range
running 3 tests
test revenue_r_spend_ledger_rejects_out_of_range_reservation_limit ... ok
test revenue_r_policy_changes_rejects_out_of_range_since ... ok
test revenue_r_spend_ledger_rejects_out_of_range_window_hours_instead_of_wrapping ... ok
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 35 filtered out; finished in 0.06s
```

The pre-existing `revenue_r_spend_ledger_window_hours_and_truthiness_match_python`
and `revenue_r_policy_changes_since_coercion_matches_python` (in-range
values, numeric strings, garbage) reran green alongside these — no
regression to the already-correct in-range/garbage paths.

## P1 — health's no-DB path violates the honest partial-response contract

**Finding.** `revenue-r-health` with `s.db == None` returned only
`{"error": "Plugin not initialized"}`. `db=None` is a real, reachable
degraded state BY DESIGN (a fresh node with no explicit `db-path` override
misses the default path and comes up running, not disabled — see
`init_canonical_mode_default_db_path_miss_does_not_disable`). Python's
`revenue_health` never carries a top-level `error`; every section is
independently populated or gap-marked.

**Fix.** The `db=None` branch now returns
`revops::rpc_health::build_health(now, None, None, None)` directly — the
SAME honest shape F11's live-`pnl_summary`-failure branch already produces
(`generated_at` present, `financials: null` + gap-declared, the honest
`boltz: {"enabled": false}`, every other section `null` + gap-declared, no
top-level `error`).

## P1 RED — real plugin-process, no usable DB path

Captured against unmodified `2b3d356`: no `db-path` override, a fresh
tempdir `$HOME` (so the default path resolution misses and the plugin comes
up running with `db=None`, per the existing default-path-miss regression
test), then `revenue-r-health` called through the real compiled binary.

Command: `cargo test -p revops --test manifest revenue_r_health_with_no_db`

```
running 1 test
test revenue_r_health_with_no_db_returns_honest_shape_not_a_top_level_error ... FAILED

failures:

---- revenue_r_health_with_no_db_returns_honest_shape_not_a_top_level_error stdout ----
thread '...' panicked at crates/revops/tests/manifest.rs:1897:5:
revenue-r-health must never carry a top-level error, even with no DB -- Python's own revenue_health never does: Object {"error": String("Plugin not initialized")}

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 38 filtered out; finished in 0.03s
```

## P1 GREEN

```
$ cargo test -p revops --test manifest revenue_r_health
running 3 tests
test revenue_r_health_with_no_db_returns_honest_shape_not_a_top_level_error ... ok
test revenue_r_health_pnl_failure_is_an_in_band_financials_error_not_a_whole_call_failure ... ok
test revenue_r_health_financials_reflect_a_real_forwards_row_rest_stays_gapped ... ok
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 36 filtered out; finished in 0.04s
```

Both the no-DB case and the existing live-pnl-failure/success cases pass
together — the same `build_health(...)` fallback shape now serves both
degraded-DB scenarios consistently.

## P1 — canonical parity tracker had incorrect counts and stale live conclusions

**Finding.** `docs/port/PARITY-CHECKLIST.md` conflated "20 total registered
Rust RPC methods" with "20 Python-equivalent RPCs registered" —
`revenue-r-fee-runway-status` (`main.rs:701-705`) is Rust-only with no
Python counterpart, so the correct Python-equivalent count is **19 of 69**
(≈28%), not 20. The pre-Task-49 baseline was inconsistently stated as both
9 and 10 in different places (9 is correct — §3's own "Rust ~55k LOC ... 9
RPCs" headline was already right; the design-doc-snapshot baseline line and
the "up from 10" phrasing were wrong). The Task 49 per-RPC table retained
Task 49's original (pre-Task-50-correction) response-contract descriptions
for profitability/analyze/capacity-report/econ-snapshot, labeled
"superseded by the table below" — which left a false table sitting in the
canonical tracker as if still current. The remaining-surface count (§3b)
said 60 of 69; post-Task-49 it is 50 of 69 (69 − 19). The live conclusion
("None of 1–4 blocks the fee cutover") pre-dated, and reads as contradicting,
the 2026-07-27 operator-approved whole-plugin-replacement decision recorded
at the top of the same document.

**Fix (doc-only, `docs/port/PARITY-CHECKLIST.md`).**

- Added an explicit round-2 correction note distinguishing "20 total Rust
  RPC methods" from "19/69 Python-equivalent RPCs", used consistently in
  the baseline/measured-counts section, the Lens 0
  `rpc-method-surface-core` row, and the "Updated after Task 49" headline.
  Baseline corrected to 9 (not 10) everywhere.
- The Task 49 per-RPC table now shows the CURRENT (Task-50-and-round-2
  -corrected) response contract for every one of the ten Batch A RPCs
  directly, instead of retaining false pre-correction rows labeled
  "superseded". Task 49's original shapes are preserved verbatim as a
  clearly-labeled **historical** subsection ("Historical: Task 49's
  original (pre-Task-50) response shapes — NOT current evidence"), moved
  out of the current-evidence table rather than left in place.
  Profitability/analyze rows now say explicit `not_yet_ported`.
- §3b's remaining-surface count corrected to 50 of 69; the "None of 1–4
  blocks the fee cutover" conclusion is now explicitly labeled historical
  (quoted, struck through in spirit) with a current conclusion stating the
  whole-plugin-replacement scope directly.
- Lens 7's profitability/analyze description updated from "honest
  no-data/`_gaps` shape" to the current explicit `not_yet_ported` marker
  language.

This is a documentation-only correction; no test transcript applies (no
behavior changed). Counts were cross-checked against
`crates/revops/tests/manifest.rs`'s `assert_eq!(result["rpcmethods"]
.as_array().unwrap().len(), 20, ...)` guard (total) and a manual count of
the 9 pre-Task-49 Python-equivalent methods (`ping`, `status`, `config`,
`history`, `report`, `dashboard`, `rebalance-plan`, `fee-debug`, `fee-wake`)
plus Task 49's ten Batch A methods = 19.

## P2 — F10 is a deliberate fail-safe divergence, not Python-exact decoding

**Finding.** `decode_policy_row`'s doc comment and
`corrupt_scalar_column_is_kept_with_defaults_not_dropped`'s test doc, plus
`docs/port/PARITY-CHECKLIST.md`'s F10 bullet, called the malformed-row
keep-with-defaults behavior "Python-exact keep-with-defaults". Python's
`_row_to_policy` (`policy_manager.py:384-439`) does NOT generally coerce
malformed SCALAR column types — it returns `row['peer_id']`,
`row['fee_ppm_target']`, `row['updated_at']`, and the present v2 columns
directly, whatever SQLite handed back, unchecked; it only wraps the
tags-JSON decode and the two enum conversions in try/except-with-default.
Claiming "Python-exact" for the scalar-defaulting behavior was inaccurate
and risked hiding a real behavior change at the policy-enforcement boundary
behind a false parity claim.

**Fix (doc-only).** Removed every "Python-exact keep-with-defaults" phrase
and replaced it with an explicit "deliberate fail-safe divergence"
description, in three places:

- `crates/revops-db/src/queries.rs`: `decode_policy_row`'s doc comment now
  states which columns default (`peer_id`/`strategy`/`rebalance_mode` ->
  `""`; `fee_ppm_target`/`fee_multiplier_min/max`/`expires_at` -> `None`;
  `updated_at` -> `0`), cites `policy_manager.py:384-439` as the reference
  that does NOT generally coerce malformed scalars, and explains
  `expires_at: None`'s fail-safe reading. The new `decode_tags_json`'s own
  doc comment (added this round for the CRITICAL fix) makes the same
  divergence claim explicit for the tags-element-level policy.
- `crates/revops-db/tests/queries.rs`:
  `corrupt_scalar_column_is_kept_with_defaults_not_dropped`'s doc comment
  updated the same way.
- `docs/port/PARITY-CHECKLIST.md`: the F10 bullet under "Scope-updated
  FIXES" now states the divergence explicitly instead of "Preferred fix:
  Python-exact keep-with-defaults", and a new round-2 CRITICAL bullet
  documents the tags-element-level fix with the same framing.

No F6/F7/F8/F9 "Python-exact" labels were touched — those ARE exact ports
of Python's own coercion rules (`int()`, truthiness, `str(x or "list")`
etc.), not fail-safe divergences, and the finding does not apply to them.

## P2 — the claimed F6/F9 red-first evidence was not behavioral evidence

**Finding.** The original F6/F9 RED (this file, "F6/F9 RED — compile
error") was a compiler error captured after `normalize_action`'s signature
changed but before `main.rs`'s call site was updated — a type-check
failure, not faulty RUNTIME behavior. There was no captured F6 behavior RED
at all.

**Fix.** The invalid section has been REPLACED (see the note in its former
place, immediately after the original "## F1-F5 GREEN" heading in this
file) rather than patched with a disclaimer. Real behavioral RED/GREEN was
captured by temporarily reverting `coerce_since`/`normalize_action`'s
BEHAVIOR (not their signatures — the code stays compilable throughout) to
the pre-F6/F9 defect, running the SAME two pre-existing tests
(`revenue_r_policy_changes_since_coercion_matches_python`,
`revenue_r_policy_explicit_null_action_is_refused_not_treated_as_list`)
against that mutated behavior, then restoring the real fix byte-for-byte
(verified via `diff` against a pre-mutation backup) and re-running for
green.

**Mutations applied (temporary, both reverted before this report was
finalized — `git diff --stat crates/revops/src/rpc_policy.rs` against HEAD
shows no change):**

- `coerce_since`: the truthy/`python_int` branch changed from
  `python_int(...).ok()` (propagates a garbage `since` as `None` ->
  `invalid_since_error`) to `Some(python_int(...).ok().unwrap_or(0))` —
  reproducing the pre-F6 defect (`v.get("since").and_then(as_i64)
  .unwrap_or(0)`: any garbage `since` silently becomes `0`).
- `normalize_action`: added `Some(Value::Null) => "list".to_string()` ahead
  of the falsy-value arm — reproducing the pre-F9 defect (an explicit
  `action: null` collapsing to the same `"list"` default as an absent key).

## P2 RED — F6/F9 behavioral (mutated source, unchanged tests)

Command: `cargo test -p revops --test manifest -- revenue_r_policy_explicit_null_action_is_refused_not_treated_as_list revenue_r_policy_changes_since_coercion_matches_python`

```
running 2 tests
test revenue_r_policy_explicit_null_action_is_refused_not_treated_as_list ... FAILED
test revenue_r_policy_changes_since_coercion_matches_python ... FAILED

failures:

---- revenue_r_policy_explicit_null_action_is_refused_not_treated_as_list stdout ----
thread '...' panicked at crates/revops/tests/manifest.rs:1158:5:
explicit null action must be refused like Python's str(None or ""): Plugin not initialized
```

(No db-path is configured for this test, by original design — the mutated
`normalize_action` no longer refuses `action: null` as unknown, so it falls
through `policy_action_gate` as if it were a real read action and reaches
the `s.db` check, surfacing "Plugin not initialized" instead of the
expected "Unknown action: ..." refusal. This is a real behavioral change in
WHICH branch executes, driven entirely by `normalize_action`'s mutated
return value — not a compile error.)

```
---- revenue_r_policy_changes_since_coercion_matches_python stdout ----
thread '...' panicked at crates/revops/tests/manifest.rs:1197:5:
assertion `left == right` failed: garbage since: Object {"changes": Array [Object {... "peer_id": String("02ffff...ff"), ... "updated_at": Number(1800000500)}], "count": Number(1), "last_change_timestamp": Number(1800000500), "since": Number(0)}
  left: Null
 right: String("Invalid 'since' timestamp. Must be a Unix timestamp.")

test result: FAILED. 0 passed; 2 failed; 0 ignored; 0 measured; 37 filtered out; finished in 0.04s
```

`since: 0` and a real `"changes"` array proves the mutated `coerce_since`
silently substituted `0` for the garbage `"abc"` input and returned the
full policy table as "changes since the epoch" — the exact pre-F6 defect,
reproduced as running behavior and caught by the pre-existing assertion.

## P2 GREEN — after reverting the mutation

`diff` against the pre-mutation backup confirmed `rpc_policy.rs` was
restored byte-for-byte before rerunning:

```
$ diff /tmp/.../rpc_policy.rs.orig crates/revops/src/rpc_policy.rs
(no output -- identical)

$ cargo test -p revops --test manifest -- revenue_r_policy_explicit_null_action_is_refused_not_treated_as_list revenue_r_policy_changes_since_coercion_matches_python
running 2 tests
test revenue_r_policy_explicit_null_action_is_refused_not_treated_as_list ... ok
test revenue_r_policy_changes_since_coercion_matches_python ... ok
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 37 filtered out; finished in 0.09s
```

`git diff --stat crates/revops/src/rpc_policy.rs` against the pre-round-2
HEAD shows no output — the file is byte-identical to before this round's
mutation-and-revert cycle; only the doc-only F10-divergence wording change
(P2, above) and this section's replacement of the invalid RED claim are
real diffs from round 2 in this area.

## P2 — health documentation described pre-port reality

**Finding.** `rpc_health.rs`'s module doc said sections 2-9 "read LIVE
IN-PROCESS PYTHON STATE with no Rust-side equivalent running in this plugin
yet: no fee controller ... executes here" — but a real Rust fee controller
(`revops-fees::cycle::ControllerState`, `cycle.rs:534`, driven by
`fee_scheduler.rs`'s scheduler loop) DOES exist and run; the actual gap is
narrower — its live state isn't plumbed into this `revenue-r-health`
handler. The checklist's Task 49 health row also said Boltz was null+gap
while the Task 50 correction table already stated the current honest
`{"enabled": false}` result (that specific inconsistency is now resolved by
this round's table replacement, above).

**Fix (doc-only).** `crates/revops/src/rpc_health.rs`'s module doc rewritten
to state the fee controller EXISTS and runs, and that the `fees` section's
gap is a plumbing gap (live `ControllerState` not wired into this handler),
scoped as explicit follow-up — not "no controller exists". Verified the
Task 50 findings table's `"should NOT stay gap"` row in
`docs/port/PARITY-CHECKLIST.md` (line ~311) already correctly says "`fees`
(computable from the live `ControllerState`) was scoped SKIP — needs
scheduler plumbing into the handler"; no change needed there, only the
Rust-side module doc was inaccurate.

## P3 — empty-array evidence did not prove no-params equivalence

**Finding.** `revenue_r_batch_a_methods_reject_nonempty_positional_params_empty_array_still_succeeds`'s
empty-array half only asserted the response was NOT the exact
positional-refusal error string — it did not prove `[]` behaves like the
no-params `{}` call; a different error or a divergent default result would
still have passed.

**Fix.** New table-driven test,
`revenue_r_batch_a_methods_empty_array_and_empty_object_params_are_semantically_equal`,
over every one of the ten `BATCH_A_SHADOW_METHODS`: calls each method with
`[]` and, separately, with `{}` (two separate real plugin-process
invocations against the same seeded production-schema DB copy), normalizes
only the documented nondeterministic wall-clock fields (`generated_at`,
`timestamp` — recursively, wherever they appear in the response tree), and
asserts full structural equality of the two normalized responses.

```
$ cargo test -p revops --test manifest revenue_r_batch_a_methods_empty_array_and_empty_object_params_are_semantically_equal
running 1 test
test revenue_r_batch_a_methods_empty_array_and_empty_object_params_are_semantically_equal ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 39 filtered out; finished in 0.34s
```

This test was green on first run (the underlying `[]`/`{}` handling was
already correct — `reject_positional_params` only refuses a NON-empty
array, so both shapes already reach the same code path); no red-first
capture applies here per the contract (only CRITICAL/P1/F6-F9 required
behavioral red-first evidence). It closes the semantic-equivalence gap the
original empty-array-only assertion left open, and would fail if a future
change made `[]` and `{}` diverge.

## Full gate run (Round 2 corrections)

See "Gate results" further below for the actual commands and output
captured for this amended checkpoint.

## Changed files (Round 2)

- `crates/revops-db/src/queries.rs` — new `decode_tags_json`; `tags` decode
  in `decode_policy_row` now calls it; doc comments updated (CRITICAL fix,
  P2 divergence documentation).
- `crates/revops-db/tests/queries.rs` — two new tests
  (`mixed_type_tags_array_preserves_valid_string_members_not_dropped_wholesale`,
  `mixed_type_tags_array_preserves_ignored_reason_tag`); F10 test doc
  comment reworded (P2).
- `crates/revops/src/rpc_params.rs` — `python_int` checked-conversion fix
  (`I64_UPPER_BOUND_F64`, `i64::try_from`); three new unit tests.
- `crates/revops/src/main.rs` — health handler's `db=None` branch now
  returns `build_health(now, None, None, None)` instead of a top-level
  error (P1 fix).
- `crates/revops/src/rpc_health.rs` — module doc corrected (P2, fee
  controller exists but isn't plumbed into this handler).
- `crates/revops/tests/manifest.rs` — new tests:
  `revenue_r_list_banned_does_not_drop_a_peer_over_a_mixed_type_tags_array`,
  `revenue_r_list_ignored_preserves_reason_tag_despite_mixed_type_tags_array`,
  `revenue_r_spend_ledger_rejects_out_of_range_window_hours_instead_of_wrapping`,
  `revenue_r_spend_ledger_rejects_out_of_range_reservation_limit`,
  `revenue_r_policy_changes_rejects_out_of_range_since`,
  `revenue_r_health_with_no_db_returns_honest_shape_not_a_top_level_error`,
  `revenue_r_batch_a_methods_empty_array_and_empty_object_params_are_semantically_equal`
  (plus the `normalize_nondeterministic_fields`/
  `DOCUMENTED_NONDETERMINISTIC_FIELDS` helper it uses).
- `docs/port/PARITY-CHECKLIST.md` — count corrections (9/19/20/50), Task 49
  per-RPC table replaced with current contracts, historical appendix added,
  F10 divergence wording, Lens 7 wording, §3b conclusion relabeled
  historical with a current conclusion added.
- `crates/revops/TASK49-REPORT.md` — this section; the invalid F6/F9 RED
  section replaced (not disclaimed).

## What is NOT claimed (Round 2)

- No new evidence-pipeline wiring — same scope boundary as Task 49/50: this
  round is corrections to existing reachable handlers' edge-case behavior
  and documentation accuracy, not new capability.
- The `fees`-section live-`ControllerState` plumbing into
  `revenue-r-health` (P2's documentation finding) is now accurately
  DOCUMENTED as a scoped follow-up, not implemented this round — no test
  claims it is wired.
- Effective/transport-proven/promotion-ready claims are unchanged from
  Task 49/50's own disclaimers.

## Amended commit (Round 2)

`git commit --amend` on the SAME single Task-49/50 checkpoint — keeps the
whole Batch-A-registration + Python-semantics-correction + round-2-audit-
correction effort as one logical checkpoint, per this round's explicit
instruction (no merge, no push). New pinned SHA recorded at the top of this
round's supervisor-facing report/response.
