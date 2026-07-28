//! Task 61 4D — the typed LN+ observer owner behind `LoopId::LnPlus`.
//!
//! One concrete pass type ([`LnPlusObserverPass`]) runs the LN+ watcher
//! (obligations: reconcile quarantined attempts, adopt/track swaps,
//! ratings retries, pending timeouts) against the Rust-owned LN+ store —
//! never Python's production database. It is spawned through the Task 57
//! bounded single-flight runtime (`ObserverPassSet::with_lnplus` →
//! `spawn_loop(LoopId::LnPlus, ..)`), so every pass rides the loop-health
//! begin/finish/fail ledger and any store persistence failure records a
//! REAL loop failure (the 4A fail-closed kernel propagates it here).
//!
//! ## Observer capability separation (structural, not disciplinary)
//!
//! This module composes ONLY the read-side observer types —
//! [`ObserverLnPlusApi`] / [`ObserverClnChain`] — whose action methods are
//! pure refusals with no inner action object. The action-capable adapter
//! types never appear here (`tests/action_surface.rs` scans this file),
//! and every kernel call additionally runs under
//! `ExecutionMode::DryRun`'s gate. Dry-run observation cannot create
//! success-shaped intents: 4B's attempt rail resolves a DryRun refusal as
//! a typed clean `NotSubmitted`, never `Committed`.
//!
//! ## Genuine planner evidence — evaluator NOT wired
//!
//! The pre-application evaluator needs REAL planner evidence
//! (`calculate_open_ev`, capex budgets, candidate scores). The Rust
//! capacity-planner runtime does not exist yet (Task 62), and fabricating
//! neutral numbers would run the gate chain on made-up economics — the
//! exact "fabricated evidence" class Task 57 forbids. So this pass runs
//! the WATCHER ONLY and records a typed skip for the evaluator; wiring
//! the evaluator is Task 62's planner-evidence deliverable. The pass
//! never issues the evaluator's signed `get_applicable_swaps` call.
//!
//! ## Config
//!
//! Runtime knobs live in the LN+ store's own `config_overrides` (the
//! Rust parallel-state analogue of Python's DB-backed config): the pass
//! re-reads them each run. `lnplus_swaps_enabled` gates whether the
//! watcher contacts LN+ at all (default OFF — a fresh observer store
//! performs zero network calls until an operator enables it).
//! `lnplus_execute_applications`/dry-run knobs are deliberately NOT read
//! here: observer composition is unconditionally `ExecutionMode::DryRun`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use revops_lnplus::exec_mode::ExecutionMode;
use revops_lnplus::loop_drivers::WatcherLoop;
use revops_lnplus::open::{OpenExecParams, DEFAULT_OPEN_COST_SATS};
use revops_lnplus::ports::{LogLevel, Logger, PeerPolicy, PolicyPort, PortError, PortResult};
use revops_lnplus::sqlite_db::SqliteLnPlusDb;

use crate::lnplus_adapters::{ObserverClnChain, ObserverLnPlusApi};

/// Watcher cadence default, matching Python's hourly `lnplus_watcher_loop`.
pub const DEFAULT_WATCHER_INTERVAL_SECS: u64 = 3600;

/// Store config key gating LN+ observation (values "true"/"1" enable).
pub const ENABLED_KEY: &str = "lnplus_swaps_enabled";

/// Everything needed to build the observer pass. All paths are the
/// caller's already-resolved values — no defaults resolve to production.
pub struct LnPlusRuntimeConfig {
    /// The Rust observer parallel-state DB (NEVER Python's production
    /// `revenue_ops.db` — main.rs enforces the collision check upstream).
    pub store_path: PathBuf,
    /// lightningd RPC socket (signer + chain reads).
    pub socket_path: PathBuf,
    /// LN+ API base (production default `http::BASE_URL`).
    pub base_url: String,
    pub http_timeout: Duration,
    pub rpc_timeout: Duration,
}

/// Plain-stderr logger for the observer pass (plugin-log routing is a
/// later polish; stderr reaches lightningd's log).
struct StderrLogger;

impl Logger for StderrLogger {
    fn log(&self, level: LogLevel, message: &str) {
        eprintln!("revops[lnplus/{level:?}]: {message}");
    }
}

/// Fail-closed stand-in for the policy rail (Task 65/66): every method
/// returns a typed error naming the missing rail. The kernel's designed
/// degradations apply — ban lookups reject fail-closed, no_close tagging
/// warns and stamps "not ours" — with the absence VISIBLE in logs, never
/// silently succeeding.
struct UnwiredPolicy;

impl PolicyPort for UnwiredPolicy {
    fn get_policy(&self, _peer: &str) -> PortResult<Option<Box<dyn PeerPolicy>>> {
        Err(PortError::new(
            "policy rail not wired (Task 65) — lookup unavailable",
        ))
    }
    fn add_tag(&self, _peer: &str, _tag: &str) -> PortResult<()> {
        Err(PortError::new(
            "policy rail not wired (Task 65) — add_tag unavailable",
        ))
    }
    fn remove_tag(&self, _peer: &str, _tag: &str) -> PortResult<()> {
        Err(PortError::new(
            "policy rail not wired (Task 65) — remove_tag unavailable",
        ))
    }
    fn is_peer_banned(&self, _pubkey: &str) -> PortResult<bool> {
        Err(PortError::new(
            "policy rail not wired (Task 65) — ban lookup unavailable (callers reject fail-closed)",
        ))
    }
}

struct Inner {
    store: Arc<SqliteLnPlusDb>,
    api: ObserverLnPlusApi,
    chain: ObserverClnChain,
    watcher: WatcherLoop,
    logger: StderrLogger,
}

impl Inner {
    fn enabled(&self) -> bool {
        use revops_lnplus::ports::LnPlusDb;
        matches!(
            self.store.get_config_override(ENABLED_KEY).as_deref(),
            Some("true") | Some("1")
        )
    }

    /// One synchronous observer pass. Any store persistence failure
    /// propagates (fail closed) and lands in loop health as a REAL
    /// failure via the owner task.
    fn run_pass(&self) -> Result<()> {
        use revops_lnplus::ports::LnPlusDb;
        if !self.enabled() {
            self.logger.log(
                LogLevel::Info,
                &format!(
                    "observer pass skipped: {ENABLED_KEY:?} is not enabled in the LN+ store — \
                     no LN+/CLN contact this pass"
                ),
            );
            return Ok(());
        }
        let now = crate::now_unix();
        let open_exec = OpenExecParams {
            estimated_cost_sats: DEFAULT_OPEN_COST_SATS,
            effective_budget_sats: None,
            budget_since_timestamp: None,
        };
        self.logger.log(
            LogLevel::Info,
            "evaluator pass skipped: planner evidence rail not wired (Task 62) — refusing to \
             run application gates on fabricated economics; watcher-only observation",
        );
        let summary = self
            .watcher
            .try_pass(
                ExecutionMode::DryRun,
                self.store.as_ref() as &dyn LnPlusDb,
                &self.api,
                &self.chain,
                &UnwiredPolicy,
                None,
                &self.logger,
                &open_exec,
                7,
                now,
            )
            .transpose()
            .map_err(|e| anyhow::anyhow!("watcher pass failed: {e}"))?;
        match summary {
            None => self.logger.log(
                LogLevel::Warn,
                "watcher pass skipped: previous pass still running (reentry guard)",
            ),
            Some(summary) => self.logger.log(
                LogLevel::Info,
                &format!(
                    "watcher pass complete: opened={} activated={} finalized={} withdrawn={} \
                     skipped={:?}",
                    summary.opened.len(),
                    summary.activated.len(),
                    summary.finalized.len(),
                    summary.withdrawn.len(),
                    summary.skipped
                ),
            ),
        }
        Ok(())
    }
}

/// The concrete LN+ observer pass `ObserverPassSet::with_lnplus` accepts.
/// Private fields; the only constructor composes the read-side observer
/// adapter types — there is no way to hand this type an action-capable
/// object.
pub struct LnPlusObserverPass {
    inner: Arc<Inner>,
    interval_secs: u64,
}

impl LnPlusObserverPass {
    pub fn observer(cfg: LnPlusRuntimeConfig) -> Result<Arc<Self>> {
        let store = Arc::new(
            SqliteLnPlusDb::open(&cfg.store_path, Box::new(StderrLogger))
                .map_err(|e| anyhow::anyhow!("open LN+ observer store: {e}"))?,
        );
        let api = ObserverLnPlusApi::new(
            cfg.base_url,
            cfg.http_timeout,
            cfg.socket_path.clone(),
            cfg.rpc_timeout,
        )
        .context("build observer LN+ api")?;
        let chain = ObserverClnChain::new(cfg.socket_path, cfg.rpc_timeout)
            .context("build observer chain reads")?;
        Ok(Arc::new(Self {
            inner: Arc::new(Inner {
                store,
                api,
                chain,
                watcher: WatcherLoop::new(),
                logger: StderrLogger,
            }),
            interval_secs: DEFAULT_WATCHER_INTERVAL_SECS,
        }))
    }

    pub fn interval_secs(&self) -> u64 {
        self.interval_secs
    }

    /// Shared handle to the LN+ store for the operator RPC surfaces (4E)
    /// — same single connection discipline, no second store.
    pub fn store(&self) -> Arc<SqliteLnPlusDb> {
        self.inner.store.clone()
    }

    /// One observer pass, synchronously. MUST be called from a blocking
    /// context (the loop owner wraps this in `spawn_blocking`; tests call
    /// it from plain threads). Public for the 4E operator paths and the
    /// local-fake integration tests — it can still only do what the
    /// observer composition can do: watch, in DryRun, via refusal-typed
    /// adapters.
    pub fn run_once_blocking(&self) -> Result<()> {
        self.inner.run_pass()
    }
}

impl crate::loop_health::ObserverPass for LnPlusObserverPass {
    fn run<'a>(
        &'a self,
        _key: crate::loop_health::RequestKey,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || inner.run_pass())
                .await
                .map_err(|join| anyhow::anyhow!("LN+ observer pass panic: {join}"))?
        })
    }
}

/// Inert-until-start cadence activation, mirroring the fee loop's
/// `FeeCadenceActivation`: nothing is spawned before plugin start.
pub struct LnPlusCadenceActivation {
    handle: crate::loop_health::LoopHandle,
    pass: Arc<LnPlusObserverPass>,
}

impl LnPlusCadenceActivation {
    pub fn new(handle: crate::loop_health::LoopHandle, pass: Arc<LnPlusObserverPass>) -> Self {
        Self { handle, pass }
    }

    pub fn activate(self) {
        let Self { handle, pass } = self;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(pass.interval_secs().max(1))).await;
                match handle
                    .request(crate::loop_health::RequestKey::from("fixed_interval"))
                    .await
                {
                    Ok(
                        crate::loop_health::Admission::Enqueued
                        | crate::loop_health::Admission::Coalesced,
                    ) => {}
                    Ok(crate::loop_health::Admission::Dropped) => {
                        eprintln!("revops: LN+ loop request dropped by bounded runtime")
                    }
                    Err(error) => {
                        eprintln!(
                            "revops: LN+ loop request persistence failed: {error:#}; trigger \
                             exiting"
                        );
                        return;
                    }
                }
            }
        });
    }
}
