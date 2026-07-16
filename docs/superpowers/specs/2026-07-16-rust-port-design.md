# cl-revenue-ops-r: Rust Port of cl_revenue_ops — Design

Date: 2026-07-16
Status: approved (Sat, 2026-07-16)
Source: 10-agent architecture mapping of cl_revenue_ops @ a9ee015 (v2.18.1); full
port map in `docs/port/port-map.json`.

## Goal

Port the cl_revenue_ops CLN plugin (~58k LOC Python: 10.4k-line entrypoint + 46
modules, 3,091 tests) to a single Rust plugin binary, replacing the Python
plugin on hive-nexus-01 (lnnode) subsystem by subsystem without breaking
production. End state: one `cl-revenue-ops-r` binary owns all subsystems —
fees, rebalancing, capacity planning, Boltz/LN+, governed economics, the full
RPC surface (36+ methods) and all 119 options — and the Python plugin is
unloaded.

## Non-goals

- No re-architecture. Behavior contracts, error strings, RPC response shapes,
  DB schema, and option names are preserved exactly. Cleanups happen after
  parity, not during.
- No multi-plugin split. The unified budget/reservation rail and governor
  require a single process with a single DB writer.
- No port of cl-mycelium integration (sunset 2026-07-16; the Python plugin is
  already standalone as of v2.17.0).

## Constraints (operator rulings, 2026-07-16)

1. **Python port-support changes live on the `port` branch** (worktree
   `~/bin/cl_revenue_ops-port`), never on `main` during the migration.
   `main` is the standing fallback: lnnode can always revert to Python main
   with a checkout + plugin restart.
2. **lnnode is the test node.** There is no staging node. Therefore: the Rust
   plugin never writes the production DB and never holds action authority
   until an explicit per-subsystem flag cutover.
3. **HARD DEADLINE (operator ruling 2026-07-16 evening): the port is
   FINISHED — all subsystems cut over, Python unloaded — by 2026-07-19.**
   Supersedes the original phased schedule. Consequences:
   - Phases 2–6 build as PARALLEL workstreams (isolated worktrees, one crate
     each) instead of sequential phases; integration is continuous.
   - Live multi-day shadow windows are replaced by (a) offline replay parity
     against recorded production data (deterministic, hours), (b) compressed
     live shadow (hours per subsystem) on lnnode, (c) staggered cutovers
     with instant-rollback flags, money-committing subsystems last.
   - NOT compressed: the 40-scenario conformance corpus byte-parity gate
     (econ core), money-path golden fixtures (htlcmax, close_protection),
     v2_state_json lossless round-trip before fee cutover, and the
     reservation no-double-spend tests. These run in minutes; they are the
     floor.
   - Risk accepted by operator: compressed live-shadow raises the chance of
     undetected behavioral divergence post-cutover. Bounded by: shared
     capital-controls ledger (daily budget caps worst-case spend), per-
     subsystem rollback flags, Python main deployable as full fallback at
     every instant through the window.

## Strategy: strangler-fig with shadow cutovers

The same playbook the Python v2.18.0 refactor used internally (`econ_shadow`),
applied cross-language:

1. Rust phase ships → deploys to lnnode in **shadow mode** for its subsystem
   (computes and records decisions; does not act).
2. **Diff period**: compare Rust decisions against Python's recorded decisions
   (both sides log full reason traces).
3. **Cutover**: disable the subsystem in Python (existing flags: fee profile,
   `planner_enabled`, `boltz_auto_cycle_enabled`, `lnplus_swaps_enabled`,
   econ governor flags), enable enforcement in Rust. Reversible per subsystem.
4. **Single-authority invariant**: a subsystem is never enabled in both
   plugins at once.

The shared sqlite database (`revenue_ops.db`, identical schema) is the reason
this is safe: one source of truth for history, budgets, and the reservation
ledger, so capital controls hold across both plugins during the transition.
During pre-cutover phases the Rust plugin opens the production DB read-only
(or a snapshot copy) and writes only to its own parallel DB file.

**db-path option (Task 8 ruling, 2026-07-16):** Python's
`revenue-ops-db-path` shadow-maps to the same `revops-r-db-path` name the
Rust observer uses for its DB probe, so the Python fixture entry is skipped
at registration and the two are deliberately the SAME conceptual option.
Cutover hazard on record: in shadow mode the Rust default is `""` (observer
opt-in, no DB), but Python's default is its standard db path — at canonical
cutover the Rust plugin MUST register `revenue-ops-db-path` with Python's
fixture default, not `""`, or an operator relying on the default would
silently lose DB access. Owned by Phase 1b (canonical-mode registration
pulls the default from fixtures/options.json for this key).

**Name collisions during coexistence:** CLN rejects a plugin that registers
an option or RPC method name another loaded plugin already owns. While both
plugins are loaded, the Rust plugin therefore registers namespaced names —
options `revops-r-<suffix>` (for Python's `revenue-ops-<suffix>`), RPC
methods `revenue-r-*` (for Python's `revenue-*`) — selected at startup via
the `REVOPS_CANONICAL_NAMES` environment switch. At final cutover (Python
unloaded) the switch flips and the canonical names apply, so operator config
files load unchanged. The diff harness owns the name mapping.

## Architecture

Cargo workspace, one binary:

```
cl-revenue-ops-r/
  Cargo.toml                 # workspace
  crates/
    revops/                  # bin: cln-plugin entrypoint, options, RPC surface,
                             # scheduler loops, bootstrap/shutdown
    revops-core/             # utils: Msat/Sat/Ppm newtypes, directional
                             # rounding (ceil fees/costs, floor balances,
                             # toward-zero deltas), SCID, canonical JSON
    revops-config/           # Config + ConfigSnapshot, validation tables,
                             # DB-persisted overrides, risk profiles
    revops-db/               # rusqlite layer: WAL, PRAGMAs, additive
                             # migrations, reservation ledger, typed queries
    revops-rpc/              # cln-rpc wrapper: timeouts, error taxonomy,
                             # tiered TTL cache (DataService)
    revops-econ/             # governed economics: types, reason codes,
                             # snapshots, intents, arbiter, governor, ledger
    revops-fees/             # DTS+PID controller, Vegas, market intel, rails
    revops-rebalance/        # planner, askrene router, native executor, engine
    revops-planner/          # capacity planner, capex, boltz, lnplus
  tools/
    diff-harness/            # RPC-output + DB-row diff against Python plugin
  docs/
```

Stack: official `cln-plugin` + `cln-rpc` crates; tokio multi-thread runtime
with `CancellationToken` shutdown (replaces `shutdown_event`), one
`tokio::select!` task per Python daemon loop (8); `rusqlite` linked against
**system** sqlite (bundled OFF) so WAL/pragma behavior matches Python;
single-owner actor tasks (mpsc) where Python held one lock across a whole
cycle (fee cycle, swap journal) — preserves the serialization the locks
provided; `serde_json` with BTreeMap ordering for canonical JSON byte-parity
(`json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=False)`);
`tokio::process` for boltzcli.

## Phases

Each phase ends with a loadable plugin and a parity gate.

1. **Foundations + read-only observer.** All 119 options registered
   (identical names), production-DB adoption (PRAGMAs, additive migrations,
   msat/sat dual-column reads), timeout-wrapped RPC layer + TTL cache,
   notification ingestion (forward_event etc.) with dedup + startup
   hydration, read-only RPC subset (revenue-status, -history, -report,
   -dashboard, -config get), heartbeat registry, clean shutdown.
   *Gate (expedited, by Jul 19): field-for-field RPC parity and ingestion
   parity vs Python on lnnode via the diff harness; schema round-trip proven
   both directions on a DB copy.*
2. **Governed econ core.** econ_types checked integer math, reason codes,
   canonical JSON, derive_seed, intents + idempotency keys, arbiter, governor,
   append-only ledger, reconcile. *Gate: the existing 40-scenario conformance
   corpus passes byte-for-byte against Rust output.*
3. **Flow/profitability/policy + budget rail.** Flow classification (Kalman),
   profitability (msat-native), policy/protection services, reservation
   lifecycle. *Gate: concurrency tests — no double-spend across restart;
   frozen datastore telemetry shapes identical.*
4. **Fee controller, dry-run first.** GaussianThompson (with legacy
   v2_state_json round-trip), PID, Vegas, market intelligence, rails in
   frozen ADR-001 order. *Gate: htlcmax golden fixtures byte-identical;
   production state blobs round-trip losslessly; N-day dry-run decision
   parity on lnnode; then fee cutover.*
5. **Rebalance stack.** Planner (golden parity first), askrene router v3,
   native executor with payment_pending semantics, engine with futility/
   cooldowns/reservations, defibrillator. *Gate: error-string contracts
   exact; shadow-priced candidates match Python; then rebalance cutover.*
6. **Capital allocation + full surface + final cutover.** Capacity planner,
   capex, Boltz cycles, LN+ state machine, remaining RPCs. *Gate:
   close_protection goldens; identical dry-run action plans on shared
   snapshots; LN+ survives kill -9 mid-obligation; Python unloaded.*

## Testing

- Strict TDD throughout; every ported behavior gets a failing test first.
- Cross-language vectors: reuse the Python repo's conformance corpus, golden
  fixtures (htlcmax, close_protection), and expected-projections as byte-level
  test fixtures in Rust CI.
- Float-math parity pinned by fixtures: Python banker's rounding vs Rust,
  toward-zero `int()`, and the hand-rolled 3x3 Cholesky/Sarrus in the DTS
  stack (highest-risk item) get dedicated fixture suites generated from the
  Python implementation on the `port` branch.
- The diff harness (tools/diff-harness) is a deliverable of Phase 1 and the
  parity instrument for every later phase.
- CI: GitHub Actions — fmt, clippy (deny warnings), test, and the fixture
  suites on every push.

## Key risks (full register in port-map.json)

- Float parity (banker's rounding, Cholesky) — fixture-driven, port-branch
  generators.
- Threading translation (cycle-spanning locks → actor tasks) — preserve
  serialization semantics, not lock shapes.
- v2_state_json legacy blobs (~50KB/channel, unknown-field preservation) —
  lossless round-trip required before any fee cutover.
- cln-plugin crate surface (dynamic options with change callbacks, 119
  options, 4 subscriptions) — verify in Phase 1 scaffold before committing;
  fallback is a thin hand-rolled JSON-RPC layer if the crate can't express
  something.
- Budget safety at each cutover — reservation state must restore with no
  double-spend and no freed budget; rehearse each cutover on a DB copy first.
