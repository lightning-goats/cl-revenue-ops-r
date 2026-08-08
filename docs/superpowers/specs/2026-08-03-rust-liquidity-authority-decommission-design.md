# Rust Liquidity Authority Decommission Design

**Date:** 2026-08-03
**Task:** Hexmem 88
**Status:** Operator-approved boundary; implementation local-only

## Goal

Make the Rust port match the approved Python v3 authority boundary: the plugin may measure and report revenue, profitability, capital efficiency, and budgets; price channels; and perform governed ordinary circular rebalancing. It must not plan or execute channel lifecycle changes, Boltz swaps, LN+ actions, or planner defibrillation.

This change is subtraction, not a dormant feature gate. Removed capability names must disappear from the build, plugin manifest, option fixture, runtime owners, schedulers, adapters, and source tree. Calls to retired RPC names must therefore receive CLN's normal method-not-found response rather than a compatibility alias or disabled tombstone.

## Removed architecture

- Remove the `revops-boltz` and `revops-lnplus` workspace crates in full.
- Reduce `revops-capital` to capex allocation/accounting. Remove its planner kernel and Boltz reservation wrapper.
- Remove the plugin's Boltz and LN+ config, process/HTTP adapters, owners, cadence/runtime passes, lifecycle roster entries, action and status RPCs, and dashboard/status assembly that claims those live integrations.
- Remove the capital planner owner, open/close/defibrillation adapters, planner evidence/discovery pipeline, planner RPCs, and the capacity-report compatibility RPC.
- Remove all corresponding plugin options and RPC parameter fixtures. Historical documents may describe the retired architecture only when explicitly marked archival; active contracts and port maps must describe the retained surface.

## Retained architecture

- `revops-capital::capex` remains a pure allocation/reporting kernel.
- `capex_evidence`, capital-efficiency calculations, capex status, total-cost budget, spend-ledger, profitability, dashboard, and other revenue reports remain read-only.
- Fee policy and execution behavior remains unchanged.
- Ordinary circular rebalance planning/execution remains behind its existing pause, authority, policy, governor, atomic reservation, daily/weekly/global/channel budget, and owner-serialization gates.
- Generic `no_close` policy metadata remains readable/writable as policy data; it no longer feeds an automatic close executor.
- Historical planner, open/close, Boltz, and LN+ tables, ledger categories, and rows remain readable. No destructive migration or data rewrite is introduced.

## Data and API compatibility

The database is append-compatible: inert historical schema and accounting categories remain because reporting may encounter old rows. Their presence must not cause any runtime owner, timer, RPC, subprocess, HTTP client, or CLN channel mutation to be constructed.

Retired RPC and option names are intentionally incompatible. This is the safety property: no alias, stub, `enabled=false` response, or hidden scheduler preserves an invocation path.

## Verification strategy

1. A source/manifest boundary test fails on the current tree because retired crates, modules, options, and RPCs still exist.
2. The same test pins retained capex, fee, policy, reporting, and rebalance surfaces.
3. Existing focused retained-core tests run throughout the removal.
4. Final verification runs `cargo fmt --all -- --check`, workspace check, clippy with warnings denied, the full workspace suite, diff hygiene, and case-insensitive source scans over active code/config/fixtures.
5. Verification is local only. No Lightning socket, node, wallet, external Boltz/LN+ service, deploy, push, or merge is permitted.

## Independent review

The Rust owner may pass Task 88's `implementation` criterion only. Codex, as the distinct Tier-1 verifier, must inspect the committed diff and rerun absence plus retained-core verification before passing `review`.
