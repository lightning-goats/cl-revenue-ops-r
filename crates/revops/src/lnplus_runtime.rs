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
    /// Task 61 4E: ONE serialization point for the owner — the scheduled
    /// watcher pass and every operator RPC take this lock, so an operator
    /// mutation never interleaves with a running pass and every RPC
    /// response is a COMPLETION acknowledgement (the store writes are
    /// acked before the lock is released).
    serial: std::sync::Mutex<()>,
    /// Python's in-memory `_last_watcher_pass` equivalent (py 2129).
    last_watcher_pass: std::sync::Mutex<Option<serde_json::Value>>,
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
        let _serial = self.serial.lock().expect("owner serialization poisoned");
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
            Some(summary) => {
                self.logger.log(
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
                );
                *self.last_watcher_pass.lock().expect("last pass poisoned") =
                    Some(serde_json::json!({
                        "at": now,
                        "opened": summary.opened,
                        "activated": summary.activated,
                        "finalized": summary.finalized,
                        "withdrawn": summary.withdrawn,
                        "skipped": summary.skipped,
                    }));
            }
        }
        Ok(())
    }
}

/// A `SwapRow` as the JSON dict Python's `_lnplus_row_to_dict` produces
/// for the status surfaces.
fn row_json(row: &revops_lnplus::db_types::SwapRow) -> serde_json::Value {
    serde_json::json!({
        "swap_id": row.swap_id,
        "status": row.status,
        "capacity_sats": row.capacity_sats,
        "duration_months": row.duration_months,
        "outbound_peer": row.outbound_peer,
        "incoming_peer": row.incoming_peer,
        "our_identifier": row.our_identifier,
        "applied_at": row.applied_at,
        "opened_at": row.opened_at,
        "ends_at": row.ends_at,
        "deadline_at": row.deadline_at,
        "channel_funding_txid": row.channel_funding_txid,
        "outcome": row.outcome,
        "tag_added": row.tag_added,
        "incoming_tag_added": row.incoming_tag_added,
        "planner_action_id": row.planner_action_id,
    })
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
                serial: std::sync::Mutex::new(()),
                last_watcher_pass: std::sync::Mutex::new(None),
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

    // -- Task 61 4E: the exact four Python-equivalent operator RPCs, each
    // -- a blocking call through the owner's serialization lock whose
    // -- return IS the completion acknowledgement (store writes acked
    // -- before the response). Response shapes mirror
    // -- cl-revenue-ops.py:4604-4676.

    /// `revenue-lnplus-status` (py 4604-4612 + get_status 2114-2131).
    pub fn operator_status(&self) -> serde_json::Value {
        let inner = &self.inner;
        let _serial = inner.serial.lock().expect("owner serialization poisoned");
        let snapshot = match revops_lnplus::watcher::get_status(inner.store.as_ref()) {
            Ok(snapshot) => snapshot,
            Err(e) => return serde_json::json!({"error": format!("LN+ store unreadable: {e}")}),
        };
        let inputs = crate::rpc_lnplus_status::LnPlusStatusInputs {
            breaker: snapshot.breaker,
            inflight: snapshot.inflight.iter().map(row_json).collect(),
            active: snapshot.active.iter().map(row_json).collect(),
            recent_ended: snapshot.recent_ended.iter().map(row_json).collect(),
            recent_failed: snapshot.recent_failed.iter().map(row_json).collect(),
            backfill_done: snapshot.backfill_done,
            last_watcher_pass: inner
                .last_watcher_pass
                .lock()
                .expect("last pass poisoned")
                .clone(),
            // The observer pass does not poll LN+ notifications (that is
            // a live-mode nicety); an empty list is the honest value.
            recent_notifications: Vec::new(),
            swaps_enabled: inner.enabled(),
            // Observer composition can NEVER execute applications — this
            // reports the structural truth, not a config knob.
            execute_applications: false,
        };
        let _ = &inner.store; // keep borrow explicit for the lock scope
        crate::rpc_lnplus_status::build_lnplus_status(Some(&inputs))
    }

    /// `revenue-lnplus-breaker-clear` (py 4615-4624): the operator path is
    /// the documented UNCONDITIONAL clear — clearing whatever is latched
    /// IS the operator's intent (automation must use the exact-cause CAS).
    pub fn operator_breaker_clear(&self) -> serde_json::Value {
        use revops_lnplus::ports::LnPlusDb;
        let inner = &self.inner;
        let _serial = inner.serial.lock().expect("owner serialization poisoned");
        let state = match inner.store.get_breaker() {
            Ok(state) => state,
            Err(e) => {
                return serde_json::json!({
                    "error": format!("breaker state unreadable (fail closed, NOT cleared): {e}")
                })
            }
        };
        let Some(state) = state else {
            return serde_json::json!({"status": "not_tripped"});
        };
        let was = state.cause.message();
        match revops_lnplus::breaker::clear_and_persist(inner.store.as_ref(), &inner.logger) {
            Ok(()) => serde_json::json!({"status": "cleared", "was": was}),
            Err(e) => serde_json::json!({"error": format!("breaker clear failed: {e}")}),
        }
    }

    /// `revenue-lnplus-abandon <swap_id>` (py 4627-4661): terminalize the
    /// row AND trip the breaker in ONE transaction (the 4A compound —
    /// Python did these as two separate writes), then best-effort
    /// `delete_application` for a still-`applied` row. In observer
    /// composition that deletion is structurally refused — logged loudly;
    /// the reconcile pending-ghost path owns the cleanup once live, which
    /// is Python's own fallback for a failed best-effort delete.
    pub fn operator_abandon(&self, swap_id: &str) -> serde_json::Value {
        use revops_lnplus::db_types::{SwapPatch, INFLIGHT_STATUSES};
        use revops_lnplus::ports::{CompoundOutcome, LnPlusApi, LnPlusDb, TerminalizeSpec};
        let inner = &self.inner;
        let _serial = inner.serial.lock().expect("owner serialization poisoned");
        let Some(row) = inner.store.get_swap(swap_id) else {
            return serde_json::json!({"error": format!("Unknown swap {swap_id}")});
        };
        if !INFLIGHT_STATUSES.contains(&row.status.as_str()) {
            return serde_json::json!({
                "error": format!("Swap {swap_id} is not in flight (status {})", row.status)
            });
        }
        let was_applied = row.status == "applied";
        let outcome = match inner.store.terminalize_and_trip(
            &TerminalizeSpec {
                swap_id,
                expected_statuses: &INFLIGHT_STATUSES,
                require_null_funding_txid: false,
            },
            &SwapPatch::default()
                .status("failed")
                .outcome("abandoned by operator"),
            revops_lnplus::breaker::BreakerCause::OperatorAbandonedSwap {
                swap_id: swap_id.to_string(),
            },
            crate::now_unix(),
        ) {
            Ok(outcome) => outcome,
            Err(e) => {
                return serde_json::json!({
                    "error": format!("abandon failed (nothing changed): {e}")
                })
            }
        };
        if let CompoundOutcome::Conflict { actual } = outcome {
            return serde_json::json!({
                "error": format!("Swap {swap_id} moved (now {actual:?}) during abandon — nothing changed")
            });
        }
        if was_applied {
            // B5(a): best-effort; a failure here is fine (the reconcile
            // pending-ghost path retries it). Observer composition
            // refuses it structurally.
            if let Err(e) = inner.api.delete_application(swap_id) {
                inner.logger.log(
                    LogLevel::Warn,
                    &format!("delete_application for abandoned swap {swap_id} (best-effort): {e}"),
                );
            }
        }
        serde_json::json!({
            "status": "abandoned",
            "swap_id": swap_id,
            "warning": "This defects on an LN+ commitment; expect a negative rating.",
        })
    }

    /// `revenue-lnplus-backfill` (py 4663-4676): adopt pre-existing LN+
    /// swaps into the local ledger via a fresh SIGNED read. Safe to run
    /// repeatedly — existing rows are never touched (typed-insert rail).
    pub fn operator_backfill(&self) -> serde_json::Value {
        use revops_lnplus::ports::LnPlusApi;
        let inner = &self.inner;
        let _serial = inner.serial.lock().expect("owner serialization poisoned");
        let my = match inner.api.get_my_swaps() {
            Ok(my) => my,
            Err(e) => {
                return serde_json::json!({"error": format!("LN+ fetch failed: {e}")});
            }
        };
        match revops_lnplus::backfill::backfill_from_lnplus(
            &my,
            inner.store.as_ref(),
            &inner.api,
            &inner.chain,
            &inner.logger,
            crate::now_unix(),
        ) {
            Ok(result) => serde_json::json!({
                "imported": {
                    "pending": result.imported.pending,
                    "opening": result.imported.opening,
                    "active": result.imported.active,
                    "ended": result.imported.ended,
                },
                "skipped": result.skipped,
                "warnings": result.warnings,
            }),
            Err(e) => serde_json::json!({
                "error": format!("backfill aborted (store persistence failure): {e}")
            }),
        }
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
