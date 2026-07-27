# revops-lnplus — wiring into `crates/revops`

This is the paste-in guide for a maintainer wiring the wiring layer built in
`src/exec_mode.rs`, `src/gated.rs`, `src/http.rs`, `src/sqlite_db.rs`, and
`src/loop_drivers.rs` into the actual `crates/revops` CLN plugin binary.
Nothing in `crates/revops-lnplus` was changed to require this — every
snippet below lives entirely outside this crate. See `ENTRYPOINTS.md` for
what is still blocked and why (CapacityPlanner, PolicyPort's tag surface).

**Read this whole file before pasting anything.** The single most important
line is in §6: passing anything other than an explicit
`ExecutionMode::Armed` keeps the whole subsystem in dry-run/no-op mode, and
that is the correct default for a first cutover.

## 0. What changed vs. `ENTRYPOINTS.md`'s original blockers

- §2 (`LnPlusDb` blocked on `revops-db` schema changes) — **resolved**:
  `sqlite_db.rs` owns its own `lnplus_swaps`/`lnplus_peers`/
  `config_overrides`/`planner_actions` schema inside THIS crate.
  `revops-db` was not modified; `revops-lnplus` now depends on it (path
  dependency) purely to reuse `revops_db::budget::BudgetDb` for the three
  unified-budget-rail methods (`reserve_spend`/etc.) rather than
  reimplementing that rail a second time.
- §2 (`LnPlusApi` needs an HTTP crate not in the workspace) — **partially
  resolved**: the transport is now behind `http::HttpTransport`, fully
  implemented and tested against a fake. No concrete HTTP crate is wired in
  — see §2 below for the one new dependency a maintainer needs to add.
- §5 (no `lnplus_*` options registered) — **already false as of this
  writing**: `fixtures/options.json` already carries all 15 `lnplus_*`
  Python options (auto-extracted from `cl-revenue-ops.py`) and
  `options_table.rs`'s generic loop in `main.rs` already registers every
  one of them under its shadow name (`revops-r-lnplus-*`). What's missing
  is resolving those already-registered values into an
  `revops_lnplus::config::LnPlusConfig` — see §4.
- §3 (CapacityPlanner) and the `PolicyPort`/`IgnorePeerPort` production
  surfaces are still genuinely blocked/unwritten. Nothing in this task
  changes that — see §5 and §7.

## 1. `Cargo.toml`

Add the path dependency to `crates/revops/Cargo.toml`:

```toml
[dependencies]
revops-lnplus = { path = "../revops-lnplus" }
```

## 2. The one new dependency this crate deliberately did NOT add

`http.rs`'s `HttpTransport`/`Signer` traits have no concrete implementation
in `revops-lnplus` (by design — see `http.rs`'s module doc: no HTTP client
crate exists anywhere in the workspace lockfile today, and "no test may
make a live HTTP request" is easiest to guarantee by not shipping a
live-capable implementation at all). A maintainer wiring this crate in
should add **`ureq`** (small, synchronous, matches this plugin's blocking
style better than an async client) to `crates/revops/Cargo.toml`:

```toml
ureq = { version = "2", default-features = false, features = ["gzip", "rustls"] }
```

and implement the transport as a thin wrapper, e.g. in a new
`crates/revops/src/lnplus_transport.rs`:

```rust
use revops_lnplus::http::{HttpMethod, HttpResponse, HttpTransport, TransportError};

pub struct UreqTransport {
    agent: ureq::Agent,
}

impl UreqTransport {
    pub fn new() -> Self {
        Self { agent: ureq::AgentBuilder::new().timeout(std::time::Duration::from_secs(30)).build() }
    }
}

impl HttpTransport for UreqTransport {
    fn request(
        &self,
        method: HttpMethod,
        url: &str,
        headers: &[(String, String)],
        body: Option<Vec<u8>>,
    ) -> Result<HttpResponse, TransportError> {
        let mut req = match method {
            HttpMethod::Get => self.agent.get(url),
            HttpMethod::Post => self.agent.post(url),
        };
        for (k, v) in headers {
            req = req.set(k, v);
        }
        let result = match body {
            Some(b) => req.send_bytes(&b),
            None => req.call(),
        };
        match result {
            Ok(resp) => {
                let status = resp.status();
                let mut buf = Vec::new();
                std::io::Read::read_to_end(&mut resp.into_reader(), &mut buf)
                    .map_err(|e| TransportError(e.to_string()))?;
                Ok(HttpResponse { status, body: buf })
            }
            // ureq treats non-2xx as `Err(ureq::Error::Status(..))`, which
            // STILL carries a real response — that is not a transport
            // failure in this crate's vocabulary (see `HttpResponse`'s doc
            // comment: non-2xx is a normal `HttpResponse`, classified by
            // `LnPlusApiClient`, not by the transport).
            Err(ureq::Error::Status(status, resp)) => {
                let mut buf = Vec::new();
                let _ = std::io::Read::read_to_end(&mut resp.into_reader(), &mut buf);
                Ok(HttpResponse { status, body: buf })
            }
            Err(ureq::Error::Transport(e)) => Err(TransportError(e.to_string())),
        }
    }
}
```

The `Signer` implementation wraps whatever CLN RPC client the plugin
already uses (see §5's async/sync note — this is the one place it bites
hardest, since `signmessage` is a live RPC call and `Signer::signmessage` is
synchronous):

```rust
use revops_lnplus::http::{SignError, Signer};

pub struct ClnSigner {
    pub socket_path: std::path::PathBuf,
    pub rt: tokio::runtime::Handle,
}

impl Signer for ClnSigner {
    fn signmessage(&self, message: &str) -> Result<String, SignError> {
        // Bridge async cln-rpc into this crate's synchronous port — see §5.
        self.rt.block_on(async {
            let mut rpc = cln_rpc::ClnRpc::new(&self.socket_path).await
                .map_err(|e| SignError(format!("connect: {e}")))?;
            let resp: serde_json::Value = rpc
                .call_raw("signmessage", &serde_json::json!({"message": message}))
                .await
                .map_err(|e| SignError(format!("signmessage: {e}")))?;
            resp.get("zbase")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .ok_or_else(|| SignError("signmessage returned no zbase field".to_string()))
        })
    }
}
```

## 3. `pub mod` lines

None needed in `crates/revops` beyond the new files you create for the
adapters above (`mod lnplus_transport;` / wherever you put `ClnSigner`,
a `ChainPort` impl, etc., in `crates/revops/src/main.rs` or `lib.rs`). This
crate (`revops-lnplus`) is consumed as an external dependency
(`use revops_lnplus::{...}`), not a submodule — there is nothing to add to
`revops-lnplus`'s own `pub mod` list (already complete: `exec_mode`,
`gated`, `http`, `loop_drivers`, `sqlite_db`, alongside the pre-existing
kernel modules).

## 4. Config options — already registered, just needs resolving

`fixtures/options.json` already has all 15 `lnplus_*` Python options
(verified 2026-07-27: `revenue-ops-lnplus-swaps-enabled`,
`-execute-applications`, `-swap-preference-margin`, `-max-duration-months`,
`-min-peer-positive-ratings`, `-min-peer-rank`, `-max-participants`,
`-min-participants`, `-apply-feerate-ceiling`, `-pending-timeout-days`,
`-inbound-credit-factor`, `-watcher-interval`, plus the shared
`revenue-ops-planner-min-channel-sats` / `-planner-max-channel-sats` /
`-planner-dry-run` / `-min-wallet-reserve`), and `options_table.rs`'s
generic loop in `main.rs` already registers every one of them under its
shadow name. Nothing needs to be ADDED to the option table.

What's missing is resolving those already-registered values into an
`revops_lnplus::config::LnPlusConfig`. The simplest correct approach,
matching the existing `configured.option(&opt)?` call sites in `main.rs`
(e.g. `fee_dryrun`/`fee_broadcast`):

```rust
fn resolve_lnplus_config(configured: &ConfiguredPlugin<SharedState, _, _>) -> revops_lnplus::config::LnPlusConfig {
    let mut cfg = revops_lnplus::config::LnPlusConfig::default();
    cfg.lnplus_swaps_enabled = configured.option(&lnplus_swaps_enabled_opt).unwrap_or(false);
    cfg.lnplus_execute_applications = configured.option(&lnplus_execute_applications_opt).unwrap_or(false);
    cfg.lnplus_apply_feerate_ceiling = configured.option(&lnplus_apply_feerate_ceiling_opt).unwrap_or(0);
    // ... one line per field, matching config.rs's own py-callsite doc comments.
    cfg
}
```

For full consistency with `revenue-r-config`'s 3-layer precedence (DB
override > `listconfigs` > fixture default — see `config_resolve.rs`'s
module doc), route these 15 fields through that module's existing
resolution helper instead of reading the Rust plugin's own shadow option
directly. That is the "do it right" version; reading the shadow option
directly is a reasonable first cut for a dry-run-only cutover (§6 means a
misresolved config value cannot cause a live action regardless).

## 5. The sync/async seam (read before wiring `ChainPort`)

Every port in `revops-lnplus::ports` is **synchronous** (`&self` methods
returning `Result` directly — matching Python's synchronous style). This
plugin (`crates/revops`) is **async** (`tokio`, `async fn main`,
`cln_rpc::ClnRpc` is an async client). A production `ChainPort` (wrapping
`revops-rpc`/`cln_rpc` per `ENTRYPOINTS.md`'s guidance) and the `Signer`
above both need to bridge that gap — `tokio::runtime::Handle::block_on`
(shown above) is the simplest option, but calling it from within an async
context deadlocks; if `evaluator_pass`/`watcher_pass` are themselves
invoked from a tokio task, run them via `tokio::task::spawn_blocking`
instead so the blocking bridge has a real OS thread to block on. This is a
real design decision for whoever wires `ChainPort` — not something this
task could resolve inside a synchronous-by-design crate.

## 6. `ExecutionMode` — read this before anything else

Every call below takes `mode: revops_lnplus::exec_mode::ExecutionMode`.
**Start with `ExecutionMode::DryRun`** (its `Default`) for the first
deployment, exactly like `revops`'s existing fee-side `fee_dryrun` gate.
`DryRun` still runs every read, every gate-chain decision, and every local
DB write (the ledger/breadcrumbs) — it only suppresses the LN+ API
mutations and `connect`/`fund_channel`. Flip to `Armed` only via an
explicit new option (do not reuse `lnplus_execute_applications` for this —
that config field is a KERNEL gate the pure evaluator already checks;
`ExecutionMode` is a SEPARATE wiring-layer gate on top of it, by design —
see `exec_mode.rs`'s module doc for why that's deliberate belt-and-suspenders,
not redundant):

```rust
let lnplus_execution_mode = if configured.option(&lnplus_armed_opt).unwrap_or(false) {
    revops_lnplus::exec_mode::ExecutionMode::Armed
} else {
    revops_lnplus::exec_mode::ExecutionMode::DryRun
};
```

(`lnplus_armed_opt` is a NEW Rust-only option you'd register — it has no
Python counterpart and does not belong in `fixtures/options.json`; suggest
`revops-r-lnplus-armed`, default `false`.)

## 7. `.rpcmethod(...)` snippets

Four operator RPCs, matching `ENTRYPOINTS.md` §4's mapping. Each needs a
`revops_lnplus::sqlite_db::SqliteLnPlusDb` handle in `SharedState` (opened
once at startup via `SqliteLnPlusDb::open(&lnplus_db_path, Box::new(logger))`,
alongside the plugin's other owned-db-file handles — never the production
`revenue_ops.db` path, see `sqlite_db.rs`'s module doc).

```rust
.rpcmethod(
    "revenue-r-lnplus-status",
    "LN+ swap automation status (breaker, in-flight, active contracts) -- read-only",
    |p: Plugin<SharedState>, _v| async move {
        let s = p.state();
        let Some(db) = &s.lnplus_db else {
            return Ok(serde_json::json!({"enabled": false}));
        };
        let status = revops_lnplus::watcher::get_status(db.as_ref());
        Ok(serde_json::json!({
            "enabled": s.lnplus_cfg.lnplus_swaps_enabled,
            "execute_applications": s.lnplus_cfg.lnplus_execute_applications,
            "execution_mode": format!("{:?}", s.lnplus_execution_mode),
            "breaker": status.breaker,
            "inflight_count": status.inflight.len(),
            "active_count": status.active.len(),
            "backfill_done": status.backfill_done,
        }))
    },
)
.rpcmethod(
    "revenue-r-lnplus-breaker-clear",
    "Clear the LN+ circuit breaker (operator acknowledgment)",
    |p: Plugin<SharedState>, _v| async move {
        let s = p.state();
        let Some(db) = &s.lnplus_db else {
            return Ok(serde_json::json!({"error": "LN+ automation not initialized"}));
        };
        let reason = revops_lnplus::breaker::tripped_message(db.as_ref());
        if reason.is_none() {
            return Ok(serde_json::json!({"status": "not_tripped"}));
        }
        revops_lnplus::breaker::clear_and_persist(db.as_ref(), s.logger.as_ref());
        Ok(serde_json::json!({"status": "cleared", "was": reason}))
    },
)
.rpcmethod(
    "revenue-r-lnplus-abandon",
    "EMERGENCY: mark an in-flight LN+ swap failed and trip the breaker (defection)",
    |p: Plugin<SharedState>, v: serde_json::Value| async move {
        let Some(swap_id) = v.get("swap_id").and_then(|s| s.as_str()) else {
            return Ok(serde_json::json!({"error": "Usage: revenue-r-lnplus-abandon <swap_id>"}));
        };
        let s = p.state();
        let Some(db) = &s.lnplus_db else {
            return Ok(serde_json::json!({"error": "LN+ automation not initialized"}));
        };
        use revops_lnplus::db_types::{SwapPatch, INFLIGHT_STATUSES};
        use revops_lnplus::ports::LnPlusDb;
        let Some(row) = db.get_swap(swap_id) else {
            return Ok(serde_json::json!({"error": format!("Unknown swap {swap_id}")}));
        };
        if !INFLIGHT_STATUSES.contains(&row.status.as_str()) {
            return Ok(serde_json::json!({"error": format!("Swap {swap_id} is not in flight (status {})", row.status)}));
        }
        db.update_swap(swap_id, &SwapPatch::default().status("failed").outcome("abandoned by operator"));
        revops_lnplus::breaker::trip_and_persist(
            db.as_ref(), s.logger.as_ref(),
            revops_lnplus::breaker::BreakerCause::LocalRowDivergentFromRemote {
                swap_id: swap_id.to_string(),
                detail: "operator abandoned swap".to_string(),
            },
            now_unix(),
        );
        // B5(a) (py comment, cl-revenue-ops.py:4647-4658): best-effort
        // delete_application if the row was still 'applied' -- go through
        // the SAME gated API the passes use, never the raw one.
        Ok(serde_json::json!({"status": "abandoned", "swap_id": swap_id}))
    },
)
.rpcmethod(
    "revenue-r-lnplus-backfill",
    "Adopt pre-existing (manually created) LN+ swaps into the local ledger",
    |p: Plugin<SharedState>, _v| async move {
        let s = p.state();
        let (Some(db), Some(api)) = (&s.lnplus_db, &s.lnplus_api) else {
            return Ok(serde_json::json!({"error": "LN+ automation not initialized"}));
        };
        // Reads only (get_my_swaps/get_swap) -- backfill never applies to
        // or withdraws anything, so it is intentionally NOT run through
        // `GatedLnPlusApi`; it is safe under every `ExecutionMode`.
        let my = match revops_lnplus::ports::LnPlusApi::get_my_swaps(api.as_ref()) {
            Ok(my) => my,
            Err(e) => return Ok(serde_json::json!({"error": format!("get_my_swaps: {e}")})),
        };
        let result = revops_lnplus::backfill::backfill_from_lnplus(
            &my, db.as_ref(), api.as_ref(), s.lnplus_chain.as_ref(), s.logger.as_ref(), now_unix(),
        );
        Ok(serde_json::json!({"imported": result.imported, "skipped": result.skipped}))
    },
)
```

## 8. Loop-spawn code

Two independent background loops, matching `ENTRYPOINTS.md` §1's split.
Both take `lnplus_execution_mode` from §6 — **never** hardcode `Armed`.

```rust
// Watcher: hourly (py default `lnplus_watcher_interval`, config.py:838),
// independent of the evaluator. `WatcherLoop` is the non-reentrancy guard
// ENTRYPOINTS.md flags as the wiring layer's job (py's `threading.Lock`,
// lnplus_swaps.py:1300) -- `run_watcher_once` itself has none.
let watcher_loop = std::sync::Arc::new(revops_lnplus::loop_drivers::WatcherLoop::new());
{
    let watcher_loop = watcher_loop.clone();
    let state = shared_state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
        loop {
            interval.tick().await;
            let state = state.clone();
            let watcher_loop = watcher_loop.clone();
            // spawn_blocking: every port call here is synchronous (§5).
            tokio::task::spawn_blocking(move || {
                let Some(db) = &state.lnplus_db else { return };
                let Some(api) = &state.lnplus_api else { return };
                let open_exec = revops_lnplus::open::OpenExecParams {
                    estimated_cost_sats: revops_lnplus::open::DEFAULT_OPEN_COST_SATS,
                    effective_budget_sats: None, // BLOCKED on CapacityPlanner -- see ENTRYPOINTS.md §3
                    budget_since_timestamp: None,
                };
                match watcher_loop.try_pass(
                    state.lnplus_execution_mode,
                    db.as_ref(), api.as_ref(), state.lnplus_chain.as_ref(),
                    state.lnplus_policy.as_ref(), None, // IgnorePeerPort -- ENTRYPOINTS.md §2, fine as None
                    state.logger.as_ref(), &open_exec,
                    state.lnplus_cfg.lnplus_pending_timeout_days, now_unix(),
                ) {
                    Some(_summary) => {}
                    None => state.logger.log(revops_lnplus::ports::LogLevel::Debug,
                        "LNPLUS: watcher pass skipped -- previous pass still running"),
                }
            }).await.ok();
        }
    });
}

// Evaluator: BLOCKED end-to-end on CapacityPlanner (ENTRYPOINTS.md §3) --
// `best_regular_ev` has no real source yet. Once a Rust CapacityPlanner
// exists, call `evaluator_pass` from ITS cycle (matching py
// `capacity_planner.py:649-653`, not a bare standalone interval), passing
// its ranked-candidate top EV as `best_regular_ev`. Until then this loop
// can run with `best_regular_ev: 0.0` (the conservative "no known regular
// alternative" value -- see `loop_drivers.rs`'s `EvaluatorPassParams` doc
// comment) if an operator wants recommend-only visibility before the
// planner ports; it must NOT be wired to `ExecutionMode::Armed` before
// CapacityPlanner exists, since `estimate_open_cost`/`capex_fleet_exploration_budget`
// both silently fall back to defaults that were never reviewed for a real
// capacity plan.
```

## 9. `SharedState` fields a maintainer will need

```rust
struct SharedState {
    // ... existing fields ...
    lnplus_db: Option<std::sync::Arc<revops_lnplus::sqlite_db::SqliteLnPlusDb>>,
    lnplus_api: Option<std::sync::Arc<dyn revops_lnplus::ports::LnPlusApi + Send + Sync>>,
    lnplus_chain: std::sync::Arc<dyn revops_lnplus::ports::ChainPort + Send + Sync>,
    lnplus_policy: std::sync::Arc<dyn revops_lnplus::ports::PolicyPort + Send + Sync>,
    lnplus_cfg: revops_lnplus::config::LnPlusConfig,
    lnplus_execution_mode: revops_lnplus::exec_mode::ExecutionMode,
    // logger: already exists for the fee side; reuse it behind a small
    // `impl revops_lnplus::ports::Logger for ...` shim (see `revops-fees`
    // for the existing pattern this crate should match).
}
```

`ports::LnPlusApi`/`ChainPort`/`PolicyPort` need `Send + Sync` bounds here
(the pure trait definitions in `ports.rs` do not require them — that is
correct, a `dyn LnPlusApi` used within one synchronous call stack never
needs to cross a thread boundary; `Send + Sync` is only needed because
`SharedState` itself is shared across the plugin's tokio tasks).

## 10. `PolicyPort` — still has no production surface

`ENTRYPOINTS.md` §2 flags this as needing a new `add_tag`/`remove_tag`/
`is_peer_banned` surface added "wherever the Rust plugin's operator-tag
store lives (likely `revops-db`)". That is unchanged by this task — nothing
here resolves it. Until it exists, a maintainer can wire
`evaluator_pass`/`watcher_pass` with `policy: None` (the evaluator gate) /
a stub `PolicyPort` that always returns `Ok(false)` from `is_peer_banned`
and `Ok(())` from `add_tag`/`remove_tag` (watcher's `activate`/`finalize`
paths require a concrete `&dyn PolicyPort`, not `Option`) — this is
explicitly sanctioned by `check_participants`'s own fail-open-when-`None`
behavior for the evaluator side, and is a deliberate placeholder (not a
production-ready policy engine) for the watcher side.
