# Rust Liquidity Authority Decommission Implementation Plan

> **For Rust owner:** Execute this plan test-first in the isolated Task 88 worktree. Do not contact production or pass the review criterion.

**Goal:** Remove Rust Boltz, LN+, CapacityPlanner, automatic open/close, and planner-defibrillation authority while retaining reporting/accounting, fees, policy, and governed ordinary rebalancing.

**Architecture:** Delete the two dedicated integration crates and the planner/executor slice of the capital crate. Remove plugin owners, adapters, schedulers, RPCs, options, and active contract entries. Keep pure capex reporting and historical accounting/schema compatibility.

**Tech Stack:** Rust 2021, Cargo workspace, `cln-plugin`, SQLite/rusqlite, integration manifest tests.

---

### Task 1: Pin the post-decommission boundary

**Files:**
- Create: `crates/revops/tests/liquidity_authority_decommission.rs`
- Modify: `crates/revops/tests/manifest.rs`

1. Add a source/workspace test asserting retired crates/modules are absent and retained modules remain.
2. Add manifest tests asserting retired RPC/option prefixes are absent in shadow and canonical modes while retained reporting, policy, fee, capex, spend-ledger, and rebalance names remain.
3. Run the new tests and record the expected red result against the current executor-bearing tree.

### Task 2: Remove dedicated executor crates and planner kernel

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `crates/revops/Cargo.toml`
- Delete: `crates/revops-boltz/**`
- Delete: `crates/revops-lnplus/**`
- Delete: `crates/revops-capital/src/boltz_reservation.rs`
- Delete: `crates/revops-capital/src/planner/**`
- Delete: corresponding executor/planner tests and entrypoint reports
- Modify: `crates/revops-capital/src/lib.rs`
- Modify: `crates/revops-capital/Cargo.toml`

1. Remove workspace/dependency edges.
2. Leave only the pure capex module and its tests in `revops-capital`.
3. Run `cargo check --workspace --all-targets` to expose remaining plugin references.

### Task 3: Remove plugin authority modules and runtime wiring

**Files:**
- Delete: `crates/revops/src/boltz_*.rs`
- Delete: `crates/revops/src/lnplus_*.rs`
- Delete: `crates/revops/src/capital_adapters.rs`
- Delete: `crates/revops/src/capital_boundaries.rs`
- Delete: `crates/revops/src/capital_candidates.rs`
- Delete: `crates/revops/src/capital_evidence.rs`
- Delete: `crates/revops/src/capital_gates.rs`
- Delete: `crates/revops/src/capital_inputs.rs`
- Delete: `crates/revops/src/capital_owner.rs`
- Delete: `crates/revops/src/capital_producers.rs`
- Delete: planner-only discovery/enrichment/open-EV/recycle modules
- Modify: `crates/revops/src/lib.rs`
- Modify: `crates/revops/src/main.rs`
- Modify: `crates/revops/src/runtime.rs`
- Modify: `crates/revops/src/lifecycle.rs`
- Modify: analytics cadence/pass modules and loop-health roster as required
- Delete/modify: corresponding integration tests

1. Remove State fields, constructors, restart reconciliation, observer passes, cadence activation, owner drain slots, and action-factory wiring.
2. Preserve fee, analytics, state writer, flow, ordinary rebalance, and read-only DB owners.
3. Compile iteratively until no removed crate/module is referenced.

### Task 4: Remove RPCs, options, and generated contracts

**Files:**
- Delete: `crates/revops/src/rpc_boltz_*.rs`
- Delete: `crates/revops/src/rpc_lnplus_status.rs`
- Delete: `crates/revops/src/rpc_planner_*.rs`
- Delete: `crates/revops/src/rpc_capacity_report.rs`
- Modify: `fixtures/options.json`
- Modify: `fixtures/config_types.json`
- Modify: `fixtures/port/rpc_params.json`
- Modify: `crates/revops/src/main.rs`
- Modify: manifest/config/RPC tests

1. Remove every retired registration and parameter/option entry, including aliases.
2. Keep capex-status, total-cost-budget, spend-ledger, policy, profitability/reporting, fee, and ordinary rebalance RPCs/options.
3. Run focused manifest and decommission tests until green.

### Task 5: Preserve historical accounting and retained core

**Files:**
- Modify only if needed: `crates/revops-db/**`
- Modify only if needed: reporting/health/dashboard/status builders
- Add/modify: retained-core tests

1. Do not drop or rename historical tables, rows, ledger categories, or generic `no_close` metadata.
2. Remove live Boltz/LN+/planner claims from status/dashboard responses without breaking retained report assembly.
3. Run focused capex, budget, fee, policy, reporting, and rebalance suites.

### Task 6: Update active documentation and parity inventory

**Files:**
- Modify: `README.md`
- Modify: `docs/port/PARITY-CHECKLIST.md`
- Modify: `docs/port/port-map.json`
- Modify/delete: active executor entrypoint/register/task reports
- Modify: package version as appropriate

1. Describe the v3 subtraction boundary and historical-data compatibility.
2. Mark old design/audit documents archival rather than rewriting evidence history.
3. Ensure active inventories contain no claim that a retired executor is available.

### Task 7: Full local verification and handoff

1. Run `cargo fmt --all -- --check`.
2. Run `cargo check --workspace --all-targets`.
3. Run `cargo clippy --workspace --all-targets -- -D warnings`.
4. Run `cargo test --workspace`.
5. Run source and manifest scans for retired names; manually classify any historical-document or inert-schema hit.
6. Run `git diff --check`, inspect `git status`, and review the complete diff.
7. Commit locally with no push/merge/deploy.
8. Pass only Hexmem Task 88 `implementation` with exact commit and command evidence, then hand the task pointer to Codex for independent review.

### Task 8: Separate Python review

1. Independently inspect Python `14f46fb..ff8c4db`, its absence/retained-core tests, and reported full-suite evidence.
2. Record the finding only on Hexmem Task 84 `review`; do not conflate it with Task 88's Rust implementation criterion.
