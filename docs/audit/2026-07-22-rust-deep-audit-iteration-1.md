# Rust deep audit — iteration 1 (2026-07-22)

> **Fix status (same day):** all highs and mediums landed, TDD'd, suite
> green (1195 tests, clippy clean) — H1 `2473056` (+ canary fixtures,
> port-repo generator `3ff1efd`), H2 `2fd282d`, M1 `1a1f18a`, M3
> `644d454`, M2 `102f2d8` (+ port-repo generator `f195f91`), M4+F3
> `526300f`. **M4 was reground during the fix:** the live main-tree
> Python checks `channel_info` FIRST (before overlay/policy) with a
> dedicated `missing_channel_info` tally, and its capture recorder
> invalidates any capture containing such a channel — the audit's
> ordering claim matched the stale `port` worktree, not production. The
> real gap was the missing tally/summary accounting, now fixed. Lesson:
> verify oracle claims against the tree production actually runs.
> Remaining lows are unfixed except F3 (tie-break, folded into `526300f`).

First deep audit of the Rust codebase itself (all prior verification was
parity-replay + shadow deployment). Six parallel audit passes, one per
subsystem, each cross-read against the Python oracle
(`/home/sat/bin/cl_revenue_ops`, and the `port` worktree
`/home/sat/bin/cl_revenue_ops-port` where Rust doc comments pin line
numbers). Every high/medium below was independently re-verified against
the code by the coordinating session before inclusion.

Baseline at audit time: commit `bc8c9be`, 1179 tests passing, clippy clean.

## Verdict

No critical findings. **2 high, 4 medium** — all are either
config-surface parity gaps or latent cutover traps; none affect current
shadow-mode behavior on the deployed default config. ~15 low/latent
findings, dominated by corrupt-blob edge divergences that fail in the
safe (closed) direction.

## High

### H1 — `x ** 2` ported as multiply/`powi(2)` instead of `py_pow(x, 2.0)` (8 sites)
`thompson/recompute.rs:274,279,485,546,639`, `thompson/dynamics.rs:356,409,474`
vs Python `fee_controller.py` (main tree: 1067, 1112, 1188, 1526, 1565,
1612–1615, 1689). CPython `**` calls libm `pow()`; a multiply diverges in
the last bit for ~0.084–0.086% of inputs (measured fact recorded in
`revops_econ::pyfloat::py_pow`'s doc, empirically re-reproduced on this
host: 1728/2M draws). `serde.rs:107,520` obeys the mandate for the same
operation; these sites don't. `.powi(2)`/`x*x` are multiplies in dev AND
release, so unlike the LLVM-rewrite trap this diverges in every build
profile. Divergence compounds: `ss` (recompute.rs:485) feeds
`noise_variance` → next cycle's regression weights → persisted posterior
drift; can occasionally flip a fee selection. The byte-exact replay
sign-off does not refute this — per-window probability of hitting a
divergent input is bounded, and passing was luck of the corpus. Fix:
route all 8 sites through `py_pow(_, 2.0)`; expect some thompson fixture
re-bakes. Follow-up: add a replay canary that exercises recompute with
known-divergent residuals.

### H2 — node-drain-bias cap wired to the wrong config knob (feature is a silent no-op)
`cycle.rs:3438-3443` passes `cfg.drain_fee_discount_max` as BOTH the
`static_max` and `bias_max` args of `drain::effective_drain_discount_max`;
Python passes the separate `node_drain_bias_max` knob (`config.py:537`,
default 0.3). `node_drain_bias_max` does not exist anywhere in the Rust
workspace (no `FeeCfgSnapshot` field, not resolved, not decoded in
replay). With `node-drain-bias-enabled=true` and the default static cap
0.0 — the feature's designed use-case — Python discounts starved-node
stagnant channels, Rust never does (`max(static, static*pressure) ==
static` for pressure ∈ [0,1]). Defaults-off today, so current shadow
parity is unaffected, but the cutover runway claims parity across the
config surface. Fix: add the `node_drain_bias_max` field through
config/resolve/replay-decode and pass it at the call site.

## Medium

### M1 — sats-EV hold-margin gate silently disabled when no `EvProvider` wired
`revops-rebalance/src/engine.rs:1884`: gate runs only when `deps.ev` is
`Some` and returns terms. In Python the gate is effectively always
evaluated for priced pairs (`_update_pair_score_decomposition` always
writes `final_score_sats`; `rebalance_engine_v2.py:1449-1521`). The
in-code comment (engine.rs:1881-1883) claiming `None` matches Python's
`final_score_present` guard is wrong — that guard only filters the
planner's bootstrap decomposition. If cutover wiring ever passes
`ev: None`, EV-negative rebalances Python rejects with
`below_hold_margin` would execute and spend real sats. Fix: make the
provider mandatory (non-Option) or fail closed (reject priced pairs)
when absent; correct the comment.

### M2 — layer-(b) bool parsing diverges from Python's strict startup cast
`config_types.rs:197-201` parses listconfigs bools with the tolerant
`{true,1,yes,on}` set for ALL fields; Python's startup cast for
`enable_vegas_reflex` / `node_drain_bias_enabled` etc. is strictly
`.lower() == 'true'` (`cl-revenue-ops.py:2610,2498` — and per-field sets
differ, e.g. `planner_enabled` accepts `{true,1,yes}`). A config file
containing `revenue-ops-vegas-reflex=1` → Python False, Rust True — and
since vegas gates a per-cycle RNG draw, the whole PyRandom stream
desyncs and the dry-run journal mismatches for a non-porting reason.
(DB-override layer (a) is correctly tolerant on both sides.) Fix:
per-field cast tables for layer (b) matching each Python startup cast.

### M3 — init-time `listconfigs` snapshot: fail-open, never retried, never refreshed
Two related gaps vs Python:
- `config_resolve.rs:233-244`: an init-time `listconfigs` failure
  (cold-start socket race, 15s timeout) degrades to an empty map with one
  stderr line, permanently — the fee controller then runs the whole
  window on fixture defaults (e.g. `min_fee_ppm=10` vs the node's
  configured 50) and nothing gates the scheduler on the fetch having
  succeeded. Deliberate fail-open (documented), but unretried.
- `main.rs:60-64,894-897`: the snapshot is cached for the plugin
  lifetime, while Python `_refresh_dynamic_config`
  (`cl-revenue-ops.py:6597-6685`) re-reads listconfigs every
  boltz/planner cycle. After `setconfig revenue-ops-min-fee-ppm-saturated=25`,
  Python fee cycles use 25 within the hour; Rust keeps the stale value
  until restart — a `dynamic:true` option silently isn't.
Fix: retry the fetch on a timer until first success (and gate the fee
scheduler or at least log loudly per-cycle until then); add a periodic
refresh for dynamic options.

### M4 — overlay/policy skip-gates run after the channel_info drop
`cycle.rs:3454-3461` drops a `channel_states` row missing from
`channels_info` before `process_channel`'s overlay/policy gates; Python
evaluates overlay → policy → THEN `channel_info` lookup
(`fee_controller.py` port:4750-4826). For a channel in flow-analysis but
absent from listpeerchannels (recently closed / transient RPC gap) with
PASSIVE policy or active overlay: Python tallies the skip and consumes
overlay/policy evidence reads; Rust does neither → strict transcript
replay fails closed with a spurious mismatch. No fee output differs.
Fix: hoist the overlay/policy gates above the channel_info drop to match
Python's order.

## Low / latent (summary — details in the per-agent transcripts)

- econ `ledger.rs:342`: unchecked `cur_reserved - cost` subtraction — the
  one spot violating the file's checked-arithmetic fail-closed policy
  (needs ~2^62 msat magnitudes).
- econ `intents.rs:73-89`: deliberate panic in `Explanation::render()` on
  float components — documented Phase-2b wiring trap; one future caller
  mistake from a crash. Consider degrading to lossy render + error log.
- thompson `dynamics.rs:244`: NaN observation fee from a corrupt blob
  panics `supported_fee_ceiling` (pyjson deliberately parses `NaN`).
- thompson serde: int-typed `posterior_mean/std` round-trip as floats;
  non-string 6th observation element dropped/shifted; corrupt-field
  recovery keeps state where Python resets fresh (all corrupt/foreign
  blob only).
- rebalance `errors.rs:81-87`: bare `"no_route"` classified
  `temporary_channel_failure` (300s cooldown) vs Python `other_retriable`
  (600s) — documented deliberate extension, real pacing divergence.
- rebalance `executor.rs:786`/`facade.rs:1097`: unclamped operator
  `max_fee_sats` overflows `*1000` (debug panic).
- fees `cycle.rs:3537-3548`: dominant-skip-reason tie-break diverges
  (BTreeMap order vs Python insertion order) — diagnostics only.
- fees `cycle.rs:607-614`: poisoned persisted `last_broadcast_fee_ppm`
  near i64::MIN overflows in `resync_broadcast_fee`; `load_cycle_state`
  clamps other fields but not this one.
- plugin `rpc_status.rs:85`: `boltz-structural-budget-sats` classified
  `"internal"`, Python says `"public"` (suffix vs field-name lookup).
- plugin: risk-profile bundle derivation + cross-field contradiction
  repairs (`config.py:919-995`) unimplemented — affects `revenue-r-config`
  reporting for profile-driven keys; crossed min/max override rows used
  raw (manual-DB-edit only).
- plugin `fee_config.rs`: layer (b) skips Python's lowercasing of
  `fee_profile`/`market_fee_mode` (capitalized value → silent default
  profile) and whitespace-padded numeric overrides are dropped where
  Python accepts them.
- plugin `notify.rs:235-247`: closure fallback stores funding txid as
  `scid` in the observer DB (unjoinable row) where Python resolves or
  skips.
- db `queries.rs:63-74`: `lifetime_aggregates` read via two statements can
  tear against a concurrent Python prune commit (transient, self-heals).
- analytics `flow.rs:667`: NaN in `hourly_out` panics `recompute_derived`
  — latent until Phase-3b DB hydration wires `TemporalProfile`.

## Verified-clean surfaces (for continuation — do not re-audit blind)

- econ: replay rules, governor keying (incl. phantom-reservation fix),
  arbiter rule order/J3 sort, reconcile classification, pyfloat
  (`py_repr`/`py_pow`/canonical JSON), sqlite byte-compat.
- thompson: streak/probe/prune bookkeeping, clamp orderings, truthiness
  fallbacks, NaN min/max positions, draw/clock stream counts.
- fees: ADR-001 stage order, PID, vegas, floors, drain multiplier,
  profiles/rails, market (weighted median, percentile, corridor),
  execution clamps, state_store merge matrix, MT19937/`pyrand`, pyjson,
  replay fail-closed paths, reason-string formats.
- rebalance: planner sizing/scoring, ceil idioms, route pricing, executor
  fee/ppm floors, partial ladder, reservation lifecycle
  (P4-007/9/16), capital controls, defib envelope, inbound-fee estimation.
- plugin: scheduler (FlushWatcher/FixedInterval), hydration field mapping,
  msat boundaries, shadow safety (production DB strictly READ_ONLY;
  `revops` crate has no dependency on rebalance's sendpay plumbing; only
  read-only RPCs registered).
- db/analytics: budget rail byte-parity (P2/P4 fixes), kalman line-parity
  (Neumaier sum, py_pow), classification branch order, telemetry key
  order.

## Recommended fix order

1. H1 (mechanical, 8 sites + fixture re-bakes) — money-path parity.
2. H2 (add missing config knob end-to-end) — config-surface parity.
3. M1 (fail closed on missing EvProvider) — cheap cutover-trap removal.
4. M3 (retry + refresh listconfigs) — cutover prerequisite.
5. M2, M4 — replay/journal noise elimination before the dry-run window.
6. Lows opportunistically alongside the crates they touch.
