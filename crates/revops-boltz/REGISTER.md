# Wiring `revops-boltz` into `crates/revops` — paste-in guide

This crate (`revops-boltz`) is a standalone library the `revops` plugin
binary does not depend on yet. This document is the exact diff shape a
maintainer pastes into `crates/revops` to reach it — written here, in
`revops-boltz`, instead of being applied directly, per the task brief's
rule that this pass touches only `crates/revops-boltz`. Nothing below has
been applied to `crates/revops`.

Read `ENTRYPOINTS.md` first for what is and is not wired inside this crate
itself (short version: `process`/`argv`/`commands`/`driver`/`rpc` are
usable end-to-end; journal persistence and the capex budget engine are
still unported, so the RPCs below can quote/status/create/claim/refund/
withdraw/wallet against a real `boltzcli`, but do not yet persist a spend
journal or draw against the unified capital budget).

## 1. `Cargo.toml`

Add the dependency to `crates/revops/Cargo.toml`'s `[dependencies]`:

```toml
revops-boltz = { path = "../revops-boltz" }
```

## 2. A new adapter module: `crates/revops/src/boltz_adapter.rs`

`revops-boltz` is a library of pure kernels plus an injected-IO wiring
layer — it does not own any process-lifetime state itself (by design, per
its HARD RULES). Something in `crates/revops` has to own:

- the real `revops_boltz::process::ProcessBoltzCli` (built once from
  config, at plugin init);
- the per-channel cooldown map (`HashMap<String, i64>`) and
  `revops_boltz::autocycle::AutoCycleErrorState`, both behind a `Mutex`
  (mirroring py's `_boltz_balance_last_action` +
  `_boltz_auto_cycle_state`, each behind their own lock);
- the resolved `BoltzCliProcessConfig` plus the operator-tunable knobs
  (budget, cooldown, max-actions, max-withdraw-cap) read once at startup
  the same way `fee_dryrun_opt`/`fee_stateful_shadow_opt` are resolved in
  `main.rs` today.

This is a NEW file, `crates/revops/src/boltz_adapter.rs`, analogous to
`fee_scheduler.rs`/`fee_execution.rs` for the fee subsystem:

```rust
//! Owns the process-lifetime state `revops-boltz`'s wiring layer needs
//! injected: the real BoltzCli adapter, the per-channel cooldown map, and
//! the auto-cycle error-state instance. See
//! `revops-boltz/REGISTER.md` for the paste-in this file implements.

use revops_boltz::autocycle::AutoCycleErrorState;
use revops_boltz::process::{BoltzCliProcessConfig, ProcessBoltzCli};
use std::collections::HashMap;
use std::sync::Mutex;

pub struct BoltzAdapterState {
    pub cli: ProcessBoltzCli,
    pub cooldown_map: Mutex<HashMap<String, i64>>,
    pub error_state: Mutex<AutoCycleErrorState>,
    pub max_withdraw_sats: i64,
    pub daily_budget_sats: i64,
    pub enforce_budget: bool,
    pub default_cooldown_seconds: i64,
    pub max_actions_per_cycle: usize,
    pub create_timeout_secs: u64,
}

impl BoltzAdapterState {
    pub fn new(config: BoltzCliProcessConfig, /* + the tunables above */) -> Self {
        Self {
            cli: ProcessBoltzCli::new(config),
            cooldown_map: Mutex::new(HashMap::new()),
            error_state: Mutex::new(AutoCycleErrorState::new()),
            // ... tunables ...
        }
    }
}
```

Register it in `crates/revops/src/lib.rs`:

```rust
pub mod boltz_adapter;
```

(next to the existing `pub mod fee_scheduler;` line)

`SharedState` in `main.rs` gets one new field, following the exact pattern
`scheduler: std::sync::OnceLock<revops::fee_scheduler::SchedulerHandle>`
already uses:

```rust
boltz: std::sync::OnceLock<revops::boltz_adapter::BoltzAdapterState>,
```

## 3. Config options (`main.rs`, next to `fee_dryrun_opt` et al.)

Field-for-field against `revops_boltz::process::BoltzCliProcessConfig` plus
the operator-tunable knobs `commands`/`driver`/`budget` need as plain
parameters (this crate never reads config itself — see its module docs).
Naming follows the existing `opt_name("suffix")` convention (`revenue-ops-*`
in canonical mode, `revops-r-*` otherwise) and mirrors the live Python
option names (`fixtures/options.json`) where they already exist:

```rust
let boltz_enabled_opt = DefaultBooleanConfigOption::new_bool_with_default(
    &opt_name("boltz-enabled"),
    false,
    "Enable the Boltz CLI integration (BoltzCliProcessConfig.enabled). \
     ProcessBoltzCli::run returns CliError::Disabled while false, \
     independent of ExecutionMode.",
);
let boltz_cli_path_opt = DefaultStringConfigOption::new_str_with_default(
    &opt_name("boltz-cli-path"),
    "/usr/local/bin/boltzcli",
    "Path to the boltzcli binary.",
);
let boltz_datadir_opt = DefaultStringConfigOption::new_str_with_default(
    &opt_name("boltz-datadir"),
    "/var/lib/boltz",
    "boltzd datadir passed to every boltzcli invocation via --datadir.",
);
let boltz_use_sudo_opt = DefaultBooleanConfigOption::new_bool_with_default(
    &opt_name("boltz-use-sudo"),
    false,
    "Prefix boltzcli invocations with `sudo -n -u <boltz-sudo-user>`.",
);
let boltz_sudo_user_opt = DefaultStringConfigOption::new_str_with_default(
    &opt_name("boltz-sudo-user"),
    "boltz",
    "sudo target user when boltz-use-sudo=true.",
);
let boltz_timeout_seconds_opt = DefaultIntegerConfigOption::new_i64_with_default(
    &opt_name("boltz-timeout-seconds"),
    60,
    "Default per-call boltzcli subprocess timeout (create-type calls internally \
     use a longer floor, matching py's max(timeout_seconds, 120/180)).",
);
let boltz_daily_budget_sats_opt = DefaultIntegerConfigOption::new_i64_with_default(
    &opt_name("boltz-daily-budget-sats"),
    0,
    "Boltz-specific daily fee budget in sats (0 = no local cap; the unified \
     capital budget, once ported, is expected to be the tighter bound in \
     practice — see revops_boltz::budget's module docs).",
);
let boltz_enforce_budget_opt = DefaultBooleanConfigOption::new_bool_with_default(
    &opt_name("boltz-enforce-budget"),
    true,
    "Reject a quote/create whose estimated fee would exceed the remaining \
     24h budget, instead of only reporting it (revops_boltz::budget::enforce_budget_for_quote).",
);
let boltz_max_withdraw_sats_opt = DefaultIntegerConfigOption::new_i64_with_default(
    &opt_name("boltz-max-withdraw-sats"),
    0,
    "Hard per-call cap for revenue-boltz-withdraw (0 = uncapped). Enforced by \
     revops_boltz::argv::withdraw_gate BEFORE any subprocess call.",
);
let boltz_cooldown_seconds_opt = DefaultIntegerConfigOption::new_i64_with_default(
    &opt_name("boltz-balance-cooldown-seconds"),
    4 * 3600,
    "Default per-channel cooldown between balance-cycle swap attempts \
     (revops_boltz::driver::run_balance_cycle_pass's default_cooldown_seconds).",
);
let boltz_max_actions_opt = DefaultIntegerConfigOption::new_i64_with_default(
    &opt_name("boltz-balance-max-actions"),
    3,
    "Max candidates ATTEMPTED per balance-cycle pass (skips do not count).",
);
```

Register with `.option(...)` alongside the existing options in the
`Builder::new(...)` chain, and resolve them into a
`revops_boltz::process::BoltzCliProcessConfig` + the adapter's other
tunables at the same point `fee_stateful_shadow_opt`'s mode-matrix
validation happens today (`main.rs`'s init path), storing the result in
`SharedState.boltz` via `OnceLock::set(...)`.

## 4. RPC methods

Every method below follows the existing `.rpcmethod(&name, "description",
|p: Plugin<SharedState>, v| async move { ... })` shape (`main.rs`
lines ~800-930). `s.boltz.get()` mirrors the existing `s.scheduler.get()`/
`s.observer_db` `Option`-checks for "adapter not initialized" safety.
`rpc_name("boltz-...")` mirrors the live Python `revenue-boltz-*` names
(shadow-mode gets `revenue-r-boltz-...`, canonical mode gets
`revenue-boltz-...`, exactly like every other RPC in `main.rs`).

### Read-only (no `ExecutionMode`, no spend)

```rust
.rpcmethod(
    &rpc_name("boltz-quote"),
    "Boltz swap fee quote (no funds moved)",
    |p: Plugin<SharedState>, v: serde_json::Value| async move {
        let Some(boltz) = p.state().boltz.get() else {
            return Ok(serde_json::json!({"error": "boltz adapter not configured"}));
        };
        let amount_sats = v.get("amount_sats").and_then(|x| x.as_i64()).unwrap_or(0);
        let swap_type_raw = v.get("swap_type").and_then(|x| x.as_str()).unwrap_or("reverse");
        let currency = v.get("currency").and_then(|x| x.as_str());
        let swap_type = match revops_boltz::argv::classify_swap_type(swap_type_raw) {
            Ok(t) => t,
            Err(e) => return Ok(serde_json::json!({"error": e.to_string()})),
        };
        let argv = match revops_boltz::argv::quote_argv(swap_type, amount_sats, currency) {
            Ok(a) => a,
            Err(e) => return Ok(serde_json::json!({"error": e.to_string()})),
        };
        let args_ref: Vec<&str> = argv.iter().map(|s| s.as_str()).collect();
        let quote_data = match revops_boltz::cli::run_json(&boltz.cli, &args_ref, 30) {
            Ok(v) => v,
            Err(e) => return Ok(serde_json::json!({"error": e.to_string()})),
        };
        // TODO: routing_fee_limit_ppm from config, only for SwapType::Reverse
        // (revops_boltz::fee::estimate_reverse_routing_fee_sats) -- 0 here.
        let currency_label = revops_boltz::argv::normalize_currency(
            currency,
            if matches!(swap_type, revops_boltz::argv::SwapType::Reverse) { "BTC" } else { "LBTC" },
        );
        Ok(revops_boltz::rpc::build_quote_response(
            swap_type_raw, amount_sats, &currency_label, &quote_data, 0,
        ))
    },
)
.rpcmethod(
    &rpc_name("boltz-status"),
    "boltzcli swapinfo for one swap id, annotated (no journal/ignore annotation yet -- see ENTRYPOINTS.md item 2)",
    |p: Plugin<SharedState>, v: serde_json::Value| async move {
        let Some(boltz) = p.state().boltz.get() else {
            return Ok(serde_json::json!({"error": "boltz adapter not configured"}));
        };
        let Some(swap_id) = v.get("swap_id").and_then(|x| x.as_str()) else {
            return Ok(serde_json::json!({"error": "missing swap_id"}));
        };
        let argv = revops_boltz::argv::swap_info_argv(swap_id);
        let args_ref: Vec<&str> = argv.iter().map(|s| s.as_str()).collect();
        let raw = match boltz.cli.run(&args_ref, 120) {
            Ok(r) => r,
            Err(e) => return Ok(serde_json::json!({"error": e.to_string()})),
        };
        let swapinfo_entry = serde_json::from_str::<serde_json::Value>(&raw)
            .ok()
            .and_then(|v| revops_boltz::parsing::primary_swap_entry(&v));
        // TODO: listswaps_entry / ignored_external_swap / journal_meta once
        // journal + ignored-external-swaps file I/O exists (ENTRYPOINTS.md item 2).
        Ok(revops_boltz::rpc::build_status_response(
            swap_id, &raw, swapinfo_entry.as_ref(), None, false, None, None,
        ))
    },
)
.rpcmethod(
    &rpc_name("boltz-wallet"),
    "boltzcli wallet balances",
    |p: Plugin<SharedState>, _v: serde_json::Value| async move {
        let Some(boltz) = p.state().boltz.get() else {
            return Ok(serde_json::json!({"error": "boltz adapter not configured"}));
        };
        let argv = revops_boltz::argv::wallet_list_argv();
        let args_ref: Vec<&str> = argv.iter().map(|s| s.as_str()).collect();
        match revops_boltz::cli::run_json(&boltz.cli, &args_ref, 30) {
            Ok(v) => Ok(v),
            Err(e) => Ok(serde_json::json!({"error": e.to_string()})),
        }
    },
)
.rpcmethod(
    &rpc_name("boltz-deposit"),
    "boltzcli wallet receive address for a resolved wallet",
    |p: Plugin<SharedState>, v: serde_json::Value| async move {
        let Some(boltz) = p.state().boltz.get() else {
            return Ok(serde_json::json!({"error": "boltz adapter not configured"}));
        };
        let currency = revops_boltz::argv::normalize_currency(
            v.get("currency").and_then(|x| x.as_str()), "LBTC",
        );
        let list_argv = revops_boltz::argv::wallet_list_argv();
        let list_args: Vec<&str> = list_argv.iter().map(|s| s.as_str()).collect();
        let wallets = match revops_boltz::cli::run_json(&boltz.cli, &list_args, 30) {
            Ok(v) => v,
            Err(e) => return Ok(serde_json::json!({"error": e.to_string()})),
        };
        let wallets_arr = wallets.get("wallets").and_then(|w| w.as_array()).cloned().unwrap_or_default();
        let Some(wallet_name) = revops_boltz::wallet::resolve_wallet_name(&wallets_arr, &currency, None, None) else {
            return Ok(serde_json::json!({"error": format!("no writable {currency} wallet found")}));
        };
        let argv = revops_boltz::argv::wallet_receive_argv(&wallet_name);
        let args_ref: Vec<&str> = argv.iter().map(|s| s.as_str()).collect();
        match boltz.cli.run(&args_ref, 30) {
            Ok(raw) => Ok(serde_json::json!({
                "wallet": wallet_name, "currency": currency,
                "address": raw.lines().last().unwrap_or("").trim(),
            })),
            Err(e) => Ok(serde_json::json!({"error": e.to_string()})),
        }
    },
)
```

### Fund-moving (`ExecutionMode`-gated — DEFAULT IS `DryRun`)

The RPC parameter that arms a live call MUST be named so a missing/absent
parameter cannot accidentally arm it — mirror py's own DD4/P1-018 pattern
(`force: bool = False`) but map it through `ExecutionMode` explicitly,
never a bare bool passed straight to a spend path:

```rust
.rpcmethod(
    &rpc_name("boltz-refund"),
    "Refund an expired/failed swap. Pass force=true to actually execute -- \
     defaults to a dry-run preview of the argv that would be run.",
    |p: Plugin<SharedState>, v: serde_json::Value| async move {
        let Some(boltz) = p.state().boltz.get() else {
            return Ok(serde_json::json!({"error": "boltz adapter not configured"}));
        };
        let Some(swap_id) = v.get("swap_id").and_then(|x| x.as_str()) else {
            return Ok(serde_json::json!({"error": "missing swap_id"}));
        };
        let destination = v.get("destination").and_then(|x| x.as_str());
        // Explicit, spelled-out arming -- see execution.rs's module docs.
        let mode = if v.get("force").and_then(|x| x.as_bool()).unwrap_or(false) {
            revops_boltz::execution::ExecutionMode::Armed
        } else {
            revops_boltz::execution::ExecutionMode::default() // DryRun
        };
        match revops_boltz::commands::execute_refund(&boltz.cli, mode, swap_id, destination, 120) {
            Ok(outcome) => Ok(serde_json::json!({"outcome": format!("{outcome:?}")})),
            Err(e) => Ok(serde_json::json!({"error": e.to_string()})),
        }
    },
)
```

`revenue-boltz-claim` and `revenue-boltz-withdraw` follow the identical
shape, calling `revops_boltz::commands::execute_claim`/`execute_withdraw`
(the latter also needs `wallet::resolve_wallet_name` + the configured
`boltz.max_withdraw_sats`, same pattern as `boltz-deposit` above).
`revenue-boltz-loop-in`/`-loop-out` follow the same shape via
`commands::execute_loop_in`/`execute_loop_out`, resolving a wallet name
first exactly as `boltz-deposit` does.

### Auto-cycle (needs the adapter's owned state)

```rust
.rpcmethod(
    &rpc_name("boltz-auto-cycle-run-now"),
    "Run one balance-cycle pass now. Requires candidates already selected \
     by a (not-yet-ported) balance-plan builder -- see ENTRYPOINTS.md item 5. \
     force=true arms live execution (default dry-run, per DD4/P1-018).",
    |p: Plugin<SharedState>, v: serde_json::Value| async move {
        let Some(boltz) = p.state().boltz.get() else {
            return Ok(serde_json::json!({"error": "boltz adapter not configured"}));
        };
        // TODO: build `candidates: Vec<revops_boltz::driver::BalanceCandidate>`
        // from a real balance-plan (unported -- ENTRYPOINTS.md item 5). Empty
        // here means this RPC is reachable and safe (a no-op pass) but not
        // yet functionally complete.
        let candidates: Vec<revops_boltz::driver::BalanceCandidate> = vec![];
        let mode = if v.get("force").and_then(|x| x.as_bool()).unwrap_or(false) {
            revops_boltz::execution::ExecutionMode::Armed
        } else {
            revops_boltz::execution::ExecutionMode::default()
        };
        let mut cooldowns = boltz.cooldown_map.lock().unwrap();
        let mut errs = boltz.error_state.lock().unwrap();
        let result = revops_boltz::driver::run_balance_cycle_pass(
            &boltz.cli, revops_core::now_unix() /* or equivalent */, mode,
            &candidates, boltz.max_actions_per_cycle, boltz.daily_budget_sats,
            boltz.default_cooldown_seconds, &mut cooldowns, &mut errs,
            boltz.create_timeout_secs,
        );
        Ok(serde_json::json!({
            "status": result.status,
            "remaining_budget_sats": result.remaining_budget_sats,
            "results": format!("{:?}", result.results),
        }))
    },
)
```

## 5. What is deliberately left as a `TODO` above

- Journal persistence around every `commands::execute_*`/
  `driver::run_balance_cycle_pass` call (`revops_boltz::journal`'s
  functions are ready; there is no file-backed store to call them from —
  `ENTRYPOINTS.md` item 2).
- The capex budget engine call inside `budget::reservation_gate`/
  `finalize_reservation_attempt` (`ENTRYPOINTS.md` item 3) — the snippets
  above use only the simpler `remaining_budget_sats -=` bookkeeping
  `driver.rs` already does.
- `revenue-boltz-history`/`revenue-boltz-budget`'s full read-path (need
  `listswaps` + journal augmentation + the external-liquidity-cost/unified-
  budget providers before calling `rpc::build_swap_history_response`/
  `build_budget_response` — `ENTRYPOINTS.md` item 4 under "Deliberately NOT
  ported").
- A real scheduler loop calling `boltz-auto-cycle-run-now`'s body on an
  interval (today it is reachable ONLY as a manual RPC, matching
  `revenue-boltz-auto-cycle-run-now`'s Python name but not yet its
  automatic-daemon sibling).
- CLN first-hop pinning for `loop-out` (deliberately out of scope for this
  crate — see `ENTRYPOINTS.md`'s "Deliberately NOT ported" #1).
