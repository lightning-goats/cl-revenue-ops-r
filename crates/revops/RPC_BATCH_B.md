# RPC Batch B — read-only operator RPCs

This batch ports 12 read-only reporting RPCs from `cl-revenue-ops.py` as
**pure response builders** (`crates/revops/src/rpc_*.rs`). Per the task
rules, `crates/revops/src/main.rs` and `crates/revops/src/lib.rs` were
**not** modified — this document is the paste-in patch a maintainer applies
to wire them into the running plugin.

`crates/revops/Cargo.toml` WAS modified (three new path deps: `revops-boltz`,
`revops-capital`, `revops-lnplus` — the same pattern `revops-rebalance`
already used). That change is already committed; nothing to paste for it.

## 1. `lib.rs` — add these `pub mod` lines

Insert alphabetically among the existing `pub mod` lines
(`crates/revops/src/lib.rs`):

```rust
pub mod rpc_boltz_budget;
pub mod rpc_boltz_history;
pub mod rpc_boltz_status;
pub mod rpc_capex_status;
pub mod rpc_econ_reconcile;
pub mod rpc_lnplus_status;
pub mod rpc_planner_candidate_sources;
pub mod rpc_planner_candidates;
pub mod rpc_planner_history;
pub mod rpc_planner_status;
pub mod rpc_rebalance_debug;
pub mod rpc_total_cost_budget;
```

This alone (no `main.rs` change) is enough to make `cargo test -p revops`
exercise every builder's unit tests, since they live in each module's own
`#[cfg(test)]` block — verified locally by temporarily adding these lines,
running the three gates, then reverting the `lib.rs` diff before committing
this batch (that temporary-wiring step is how every number in the PR
report below was actually measured, not guessed).

## 2. `main.rs` — per-RPC handler snippets

Each snippet below is a `.rpcmethod(...)` block in the same chain
`main.rs` already builds (see the existing `rebalance_plan_name`/
`config_name` handlers for the surrounding pattern: a `&<name>_name`
binding registered earlier via `plugin.method(...)`, then a closure body
here). **Fetches marked `// GAP:` do not exist anywhere in this Rust
workspace yet** — the snippet shows the shape the caller needs to
assemble, not a working implementation. An RPC with every input gapped is
listed as "cannot be wired today" below; wire it once the underlying
`revops-db` reader (or subprocess adapter) exists.

---

### `revenue-planner-status` → `rpc_planner_status::build_planner_status`

**Cannot be wired today.** Every input (`enabled`, `dry_run`,
`execute_closes`, `max_closes_per_cycle`, `candidate_pool_size`,
`recent_actions`) requires either a new config struct reading
`planner_enabled`/`planner_dry_run`/`planner_execute_closes`/
`planner_max_closes_per_cycle`, or a new `revops-db` reader for the
`planner_candidates`/`planner_actions` tables — neither exists.

```rust
.rpcmethod(
    &planner_status_name,
    "capacity planner status: pending actions, config",
    |_p: Plugin<SharedState>, _v: serde_json::Value| async move {
        // GAP: no revops-db reader for planner_candidates/planner_actions;
        // no config struct for the four planner_* flags below.
        Ok(revops::rpc_planner_status::build_planner_status(
            &revops::rpc_planner_status::PlannerStatusInputs {
                enabled: false,           // GAP: cfg.planner_enabled
                dry_run: false,           // GAP: cfg.planner_dry_run
                execute_closes: false,    // GAP: cfg.planner_execute_closes
                max_closes_per_cycle: 0,  // GAP: cfg.planner_max_closes_per_cycle
                candidate_pool_size: 0,   // GAP: len(db.get_planner_candidates())
                recent_actions: vec![],   // GAP: db.get_planner_actions(limit=5)
            },
        ))
    },
)
```

### `revenue-planner-candidates` → `rpc_planner_candidates::build_planner_candidates`

**Cannot be wired today** (needs a `planner_candidates` DB reader). Limit
parsing IS ready: `rpc_planner_candidates::parse_query_limit(v.get("limit"), 20, 1, 1000)`.

```rust
.rpcmethod(
    &planner_candidates_name,
    "list scored peer candidates for channel opens",
    |_p: Plugin<SharedState>, v: serde_json::Value| async move {
        let limit = match revops::rpc_planner_candidates::parse_query_limit(
            v.get("limit"), 20, 1, 1000,
        ) {
            Ok(l) => l,
            Err(e) => return Ok(e),
        };
        // GAP: database.get_planner_candidates(limit=limit) has no
        // revops-db reader yet.
        let _ = limit;
        Ok(revops::rpc_planner_candidates::build_planner_candidates(vec![]))
    },
)
```

### `revenue-planner-candidate-sources` → `rpc_planner_candidate_sources::build_planner_candidate_sources`

**Cannot be wired today** (same `planner_candidates` reader gap; also
needs `database.get_planner_candidates(min_score=-999.0, limit=100)`,
a different call shape than the plain `-candidates` RPC uses).

```rust
.rpcmethod(
    &planner_candidate_sources_name,
    "strategy distribution of the current candidate pool",
    |_p: Plugin<SharedState>, _v: serde_json::Value| async move {
        // GAP: database.get_planner_candidates(min_score=-999.0, limit=100)
        let rows: Vec<revops::rpc_planner_candidate_sources::CandidateRow> = vec![];
        Ok(revops::rpc_planner_candidate_sources::build_planner_candidate_sources(&rows))
    },
)
```

### `revenue-planner-history` → `rpc_planner_history::build_planner_history`

**Cannot be wired today** (needs a `planner_actions` DB reader). Limit
parsing reuses the same shared helper as `-candidates`.

```rust
.rpcmethod(
    &planner_history_name,
    "audit log of past planner actions",
    |_p: Plugin<SharedState>, v: serde_json::Value| async move {
        let limit = match revops::rpc_planner_candidates::parse_query_limit(
            v.get("limit"), 20, 1, 1000,
        ) {
            Ok(l) => l,
            Err(e) => return Ok(e),
        };
        // GAP: database.get_planner_actions(limit=limit) has no
        // revops-db reader yet.
        let _ = limit;
        Ok(revops::rpc_planner_history::build_planner_history(vec![]))
    },
)
```

### `revenue-capex-status` → `rpc_capex_status::build_capex_status`

**Computation is fully ported and callable**, but only once a real
`CapexAllocations` exists — building that needs `revops_capital::capex::
CapexEvidence` (per-channel `ChannelProfile`, `bleeder_status`,
`capex_by_channel_sats`, `spend_summary`, `success_rates`, `fleet_efficiency`
— see `crates/revops-capital/tests/capex.rs` for the full fixture shape),
none of which is read from a live DB/RPC anywhere in this workspace yet.

```rust
.rpcmethod(
    &capex_status_name,
    "unified capex budget allocations",
    |_p: Plugin<SharedState>, _v: serde_json::Value| async move {
        // GAP: CapexEvidence gathering (analyze_all_channels(), bleeder
        // status, get_total_capex_by_channel(30), get_spend_ledger_summary(30),
        // confirmed on-chain balance, capital-efficiency snapshot) is
        // unported -- there is nowhere to get a real CapexAllocations from
        // yet. Once it exists:
        //   let alloc = revops_capital::capex::compute_allocations(&evidence, &cfg);
        //   Ok(revops::rpc_capex_status::build_capex_status(&alloc, revops::now_unix()))
        Ok(serde_json::json!({"error": "Capex engine not initialized"}))
    },
)
```

### `revenue-total-cost-budget` → `rpc_total_cost_budget::build_total_cost_budget`

**Mostly cannot be wired today.** Only `growth_budget`/`mode`/
`effective_budget_sats`/`remaining_sats` become real once ALL FIVE cost
categories are known; today none are. See the module doc comment in
`rpc_total_cost_budget.rs` for the full list of missing `revops-db`
readers (`get_total_routing_revenue`, `get_opening_costs_since`,
`get_closure_costs_since`, `get_daily_rebalance_spend`,
`get_spend_ledger_summary`, `get_cost_evidence_coverage`) and the missing
`boltz_manager.get_boltz_cost_components` subprocess adapter. The builder
itself is fully testable and correct today (13 passing unit tests) — it
degrades to an almost-entirely-`_phase1b_gaps` response when called with
`Default::default()` components, which IS the honest current answer.

```rust
.rpcmethod(
    &total_cost_budget_name,
    "unified budget status across rebalances, Boltz, and on-chain liquidity costs",
    |_p: Plugin<SharedState>, v: serde_json::Value| async move {
        let wh = match revops::rpc_total_cost_budget::parse_window_hours(v.get("window_hours")) {
            Ok(w) => w,
            Err(e) => return Ok(e),
        };
        // GAP: every Option field below needs a revops-db reader or a
        // boltz subprocess adapter that does not exist yet (see module
        // doc comment). daily_budget_sats/growth_* ARE resolvable now via
        // the existing config machinery (config_resolve.rs) -- wire those
        // even before the DB readers land, so `mode`/`effective_budget_sats`
        // at least reflect real config once actual_total is available.
        let inputs = revops::rpc_total_cost_budget::TotalCostBudgetInputs {
            now: revops::now_unix(),
            daily_budget_sats: 0,       // wire from cfg.daily_budget_sats
            growth_enabled: false,      // wire from cfg.growth_budget_enabled
            growth_earned_fraction: 0.25,
            growth_experiment_fraction: 0.10,
            growth_max_extra_sats: 0,
            growth_hard_ceiling_sats: 0,
            ..Default::default()
        };
        Ok(revops::rpc_total_cost_budget::build_total_cost_budget(wh, &inputs))
    },
)
```

### `revenue-lnplus-status` → `rpc_lnplus_status::build_lnplus_status`

**Cannot be wired today** (no LN+ lifecycle DB reads are wired into
`main.rs`; `revops-lnplus` is a decision-kernel-only crate per its
`ENTRYPOINTS.md`). Once `lnplus_inflight_swaps`/`lnplus_get_swaps_by_status`
readers exist:

```rust
.rpcmethod(
    &lnplus_status_name,
    "LN+ swap automation status: breaker, in-flight, active contracts",
    |_p: Plugin<SharedState>, _v: serde_json::Value| async move {
        // GAP: no revops-db readers for lnplus_swaps/lnplus_config_overrides.
        Ok(revops::rpc_lnplus_status::build_lnplus_status(None))
    },
)
```

### `revenue-boltz-status <swap_id>` → `rpc_boltz_status::build_boltz_status`

Needs a `boltzcli` subprocess adapter (`swapinfo -- <id>`, `listswaps
--json`) plus the two local JSON file reads (ignored-external-swaps,
swap journal) — none exist in this Rust workspace (no subprocess
execution anywhere in `crates/revops` today). The annotation/shaping
logic itself is fully ported and tested.

```rust
.rpcmethod(
    &boltz_status_name,
    "per-swap Boltz status",
    |_p: Plugin<SharedState>, v: serde_json::Value| async move {
        let Some(swap_id) = v.get("swap_id").and_then(|s| s.as_str()) else {
            return Ok(revops::rpc_boltz_status::build_boltz_status_usage_error());
        };
        // GAP: no `boltzcli` subprocess adapter, no ignored/journal file
        // readers anywhere in this workspace yet.
        let inputs = revops::rpc_boltz_status::BoltzSwapStatusInputs {
            swap_id: swap_id.to_string(),
            swapinfo_raw: String::new(),
            swapinfo_entry: None,
            listswaps_entry: None,
            ignore_meta: None,
            journal_meta: None,
        };
        Ok(revops::rpc_boltz_status::build_boltz_status(&inputs))
    },
)
```

### `revenue-boltz-history [limit]` → `rpc_boltz_history::build_boltz_history`

Needs the same `boltzcli listswaps --json` + journal-augmentation
adapter as `-status`. The sort/limit/`cost_summary` math is fully ported.

```rust
.rpcmethod(
    &boltz_history_name,
    "Boltz swap history with cost summary",
    |_p: Plugin<SharedState>, v: serde_json::Value| async move {
        let limit = v.get("limit").and_then(|l| l.as_i64());
        // GAP: no `boltzcli listswaps --json` + journal-augmentation
        // adapter anywhere in this workspace yet.
        let swaps: Vec<serde_json::Map<String, serde_json::Value>> = vec![];
        Ok(revops::rpc_boltz_history::build_boltz_history(swaps, limit))
    },
)
```

### `revenue-boltz-budget` → `rpc_boltz_budget::build_boltz_budget`

Needs the manual-only, journal-augmented swap list (same adapter gap as
above) to compute `revops_boltz::budget::boltz_cost_components`, plus a
resolved unified `daily_budget_sats` and `ExternalLiquidityCosts`
(currently only reachable through the also-unported
`_get_external_liquidity_costs`/`_get_global_budget_limit` providers).
The `budget_status` math itself is fully ported.

```rust
.rpcmethod(
    &boltz_budget_name,
    "Boltz swap budget status",
    |_p: Plugin<SharedState>, _v: serde_json::Value| async move {
        // GAP: swap-list adapter (see -status/-history), external
        // liquidity cost provider, and global budget limit provider are
        // all unported.
        let inputs = revops::rpc_boltz_budget::BoltzBudgetInputs {
            daily_budget_sats: 0,
            local: revops_boltz::budget::CostComponents::default(),
            external: revops_boltz::budget::ExternalLiquidityCosts::default(),
            external_liquidity_costs: serde_json::json!({"source": "none"}),
            budget_info: serde_json::json!({}),
            enforce_budget: false, // wire from cfg.boltz_enforce_budget
        };
        Ok(revops::rpc_boltz_budget::build_boltz_budget(&inputs))
    },
)
```

### `revenue-rebalance-debug` → `rpc_rebalance_debug::build_rebalance_debug`

**Partially wireable today**, unlike the other RPCs in this batch: the
channel bucketing needs only a `listpeerchannels` call (already proven
plumbing — see the existing `revenue-r-rebalance-plan` handler for the
exact `cln_rpc::ClnRpc` + `listpeerchannels` pattern to copy) plus the
three threshold config fields (already resolvable via
`config_resolve.rs`/`config_types.rs`, the same machinery
`revenue-r-config` uses). `capital_controls`/`last_decision`/`last_cycle`/
`drain_demand` stay `null` (see the module doc comment for exactly why
each is unported).

```rust
.rpcmethod(
    &rebalance_debug_name,
    "diagnose why rebalancing may not be happening",
    |p: Plugin<SharedState>, v: serde_json::Value| async move {
        let filters = revops::rpc_rebalance_debug::RebalanceDebugFilters {
            channel_id: v.get("channel_id").and_then(|s| s.as_str()).map(str::to_string),
            peer_id: v.get("peer_id").and_then(|s| s.as_str()).map(str::to_string),
            summary_only: v.get("summary_only").and_then(|b| b.as_bool()).unwrap_or(false),
            include_hot_markers: v.get("include_hot_markers").and_then(|b| b.as_bool()).unwrap_or(true),
            max_candidates: revops::rpc_rebalance_debug::parse_max_candidates(v.get("max_candidates")),
        };
        // Real plumbing, same pattern as revenue-r-rebalance-plan's handler:
        let cfg = p.configuration();
        let socket = std::path::PathBuf::from(&cfg.lightning_dir).join(&cfg.rpc_file);
        let mut rpc = match cln_rpc::ClnRpc::new(&socket).await {
            Ok(r) => r,
            Err(e) => return Ok(serde_json::json!({"error": format!("connect {}: {e}", socket.display())})),
        };
        let resp: serde_json::Value = match rpc.call_raw("listpeerchannels", &serde_json::json!({})).await {
            Ok(v) => v,
            Err(e) => return Ok(serde_json::json!({"error": format!("listpeerchannels: {e}")})),
        };
        let channels: Vec<_> = resp.get("channels").and_then(|c| c.as_array())
            .map(|a| a.as_slice()).unwrap_or(&[]).iter().filter_map(|c| {
                // GAP: fee_ppm needs listpeerchannels' `fee_base_msat`/
                // `fee_proportional_millionths` on the outgoing side; wire
                // alongside capacity/spendable, same fields
                // rpc_rebalance::planner_channel_from_rpc already reads.
                let channel_id = c.get("short_channel_id")?.as_str().map(revops::notify::normalize_scid)?;
                Some(revops::rpc_rebalance_debug::RebalanceDebugChannel {
                    channel_id,
                    peer_id: c.get("peer_id")?.as_str()?.to_string(),
                    capacity_sats: 0,    // wire: total_msat / 1000
                    spendable_sats: 0,   // wire: spendable_msat / 1000
                    fee_ppm: 0,          // wire: fee_proportional_millionths
                })
            }).collect();
        // GAP: dry_run (cfg.dry_run), thresholds (cfg.low/high_liquidity_threshold,
        // cfg.rebalance_hold_margin -- all resolvable via config_resolve.rs but
        // not wired in this sketch), active_channel_ids (no job manager exists),
        // hot_override_depletion_thresholds (a plain DB table read, unported).
        Ok(revops::rpc_rebalance_debug::build_rebalance_debug(
            &filters,
            false,
            &revops::rpc_rebalance_debug::RebalanceDebugThresholds {
                low_liquidity_threshold: 0.3,
                high_liquidity_threshold: 0.7,
                rebalance_hold_margin_sats: 0.0,
            },
            &channels,
            &std::collections::BTreeSet::new(),
            &std::collections::BTreeMap::new(),
        ))
    },
)
```

### `revenue-econ-reconcile [apply] [stale_after_seconds]` → `rpc_econ_reconcile::*`

**Wireable today** if `econ_shadow`/`EconLedger`/`revops-db`'s
`spend_reservations` reader are already live elsewhere in this binary
(this batch did not check; `revops-econ` is already a `[dependencies]`
entry of `revops`, unlike the other four new crates this batch adds).

```rust
.rpcmethod(
    &econ_reconcile_name,
    "reconcile the econ ledger against spend_reservations truth",
    |_p: Plugin<SharedState>, v: serde_json::Value| async move {
        // GAP: econ_shadow enablement check, EconLedger handle, and a
        // `database.get_spend_reservation_states()` reader are this
        // handler's job -- confirm whether they already exist elsewhere
        // in main.rs/hydration.rs before assuming this needs new plumbing.
        // if !econ_shadow_enabled {
        //     return Ok(revops::rpc_econ_reconcile::build_econ_reconcile_disabled());
        // }
        // let Some((ledger, db_states)) = ... else {
        //     return Ok(revops::rpc_econ_reconcile::build_econ_reconcile_unavailable());
        // };
        let stale_after = v.get("stale_after_seconds").and_then(|s| s.as_i64())
            .unwrap_or(3600).max(60);
        let apply = v.get("apply").and_then(|a| a.as_bool()).unwrap_or(false);
        let _ = (stale_after, apply);
        Ok(revops::rpc_econ_reconcile::build_econ_reconcile_unavailable())
    },
)
```

## 3. Summary: what's wireable today vs. blocked

| RPC | Builder tested? | Wireable with existing plumbing? |
|---|---|---|
| `revenue-planner-status` | yes | no — needs planner DB readers + config struct |
| `revenue-planner-candidates` | yes | no — needs `planner_candidates` DB reader (limit parsing ready) |
| `revenue-planner-candidate-sources` | yes | no — needs `planner_candidates` DB reader |
| `revenue-planner-history` | yes | no — needs `planner_actions` DB reader (limit parsing ready) |
| `revenue-capex-status` | yes | no — needs `CapexEvidence` gathering (5 DB/analysis reads) |
| `revenue-total-cost-budget` | yes | no — needs 6 DB readers + Boltz cost-component adapter |
| `revenue-lnplus-status` | yes | no — needs LN+ DB readers |
| `revenue-boltz-status` | yes | no — needs `boltzcli` subprocess adapter + 2 file readers |
| `revenue-boltz-history` | yes | no — needs same `boltzcli` adapter |
| `revenue-boltz-budget` | yes | no — needs same adapter + 2 budget providers |
| `revenue-rebalance-debug` | yes | **partially** — channel bucketing wireable via existing `listpeerchannels` pattern; capital_controls/last_decision/last_cycle/drain_demand stay gapped |
| `revenue-econ-reconcile` | yes | maybe — depends on whether `econ_shadow`/ledger/DB handles already exist elsewhere in `main.rs` (not checked by this batch) |

None of these 12 RPCs can be fully wired without at least one new
`revops-db` reader, subprocess adapter, or evidence-gathering module —
this batch's scope was the pure response-shaping layer, per the task
brief. The next batch to close this gap should prioritize the
`revops-db` readers for `planner_candidates`/`planner_actions` (small,
mechanical, unblocks 4 of the 12 RPCs at once).
