//! Task 71 / F71-R16: the three CONCRETE analytics observer passes.
//!
//! `flow_owner`, `startup_snapshot` and `financial_snapshot` are pure
//! planners: they take `Result`-shaped evidence and return either a typed
//! refusal or a plan. They had no production caller, so on a running node
//! FlowAnalysis / StartupSnapshot / FinancialSnapshot stayed `NotWired`
//! forever. This module is the missing half — the concrete
//! [`crate::loop_health::ObserverPass`] implementations that read the real
//! sources, drive those planners, and route the result to the Rust-owned
//! observer store.
//!
//! **Capability posture (F71-R17).** Each pass is its OWN concrete type,
//! exactly as `FeeObserverPass` and `LnPlusObserverPass` are. The
//! `ObserverPass` trait stays `pub(crate)` and the runtime builders accept
//! `Arc<ConcreteType>`, so no external crate can inject an arbitrary
//! action-bearing pass into the observer composition. These three hold no
//! action capability at all: they issue read-only RPCs, run frozen
//! kernels, and write only to the plugin's own database. The Task 69
//! capital-execution boundary is untouched.
//!
//! **Refusals fail the pass.** Every planner refusal is returned as an
//! `Err` carrying its stable refusal code, so the loop owner records a
//! FAILED generation for this boot. Swallowing a refusal would leave a
//! `Passed` loop that observed nothing — the exact false-health shape
//! Task 67 set out to remove.
//!
//! **One pass, one transaction (F71-R18).** The flow pass persists every
//! derived state row and every updated Kalman state through a single
//! `persist_flow_pass` store call. Per-row writes were the first draft and
//! they were wrong: `rust_kalman_state` carries no `boot_id` and no
//! provenance of any kind, so a partially written filter set is
//! indistinguishable from a complete one, and the next pass would resume
//! some channels from this boot and others from a previous boot —
//! silently, and permanently.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::pin::Pin;

use anyhow::{Context, Result};
use revops_analytics::flow::EmaBucket;
use revops_analytics::kalman::{DailyBucket, KalmanFlowState, KalmanStateDict};
use revops_db::owner::ObserverHandle;
use serde_json::{json, Value};

use crate::financial_snapshot::{
    plan_financial_snapshot, FinancialDeps, LifetimeStats, STARTUP_DELAY_SECONDS,
};
use crate::flow_owner::{run_flow_pass, ChannelHistory, FlowDeps};
use crate::loop_health::{ObserverPass, RequestKey};
use crate::startup_snapshot::{
    plan_startup_snapshot, SnapshotDeps, RECENT_HISTORY_WINDOW_SECONDS, SNAPSHOT_EVENT_TYPE,
};

/// py `flow_analysis_loop`'s staggered startup delay
/// (cl-revenue-ops.py:3173).
pub const FLOW_STARTUP_DELAY_SECONDS: u64 = 30;

/// py `max(60, cfg.flow_interval)` with `flow_interval` defaulting to
/// 3600 (config.py:505).
pub const FLOW_MIN_INTERVAL_SECONDS: u64 = 60;
pub const FLOW_DEFAULT_INTERVAL_SECONDS: u64 = 3_600;

/// py `snapshot_peers_delayed`'s one-shot delay (cl-revenue-ops.py:3469).
pub const STARTUP_SNAPSHOT_DELAY_SECONDS: u64 = 60;

/// Re-exported so `main.rs` schedules the financial snapshot on py's own
/// cadence rather than restating the number.
pub use crate::financial_snapshot::SNAPSHOT_INTERVAL_SECONDS as FINANCIAL_INTERVAL_SECONDS;
pub const FINANCIAL_STARTUP_DELAY_SECONDS: u64 = STARTUP_DELAY_SECONDS as u64;

const RPC_TIMEOUT_SECONDS: u64 = 30;

/// One read-only RPC, timeout-bounded. Errors are stringly-typed on
/// purpose: they flow straight into the planners' `Result`-shaped inputs,
/// which is where they become typed refusals naming the failed source.
async fn read_rpc(
    socket_path: &Path,
    method: &'static str,
    params: Value,
) -> Result<Value, String> {
    let call = async {
        let mut rpc = cln_rpc::ClnRpc::new(socket_path)
            .await
            .with_context(|| format!("connect lightning-rpc socket {}", socket_path.display()))?;
        rpc.call_raw::<Value, Value>(method, &params)
            .await
            .map_err(|e| anyhow::anyhow!("{method} RPC error: {e}"))
    };
    revops_rpc::call_with_timeout(method, RPC_TIMEOUT_SECONDS, call)
        .await
        .map_err(|error| format!("{error}"))
}

// =====================================================================
// FlowAnalysisPass
// =====================================================================

/// The tunables py reads from config for each flow pass.
#[derive(Clone, Copy, Debug)]
pub struct FlowPassConfig {
    /// py `source_threshold` (config.py:597).
    pub source_threshold: f64,
    /// py `sink_threshold` (config.py:598).
    pub sink_threshold: f64,
    /// py `flow_window_days` (config.py:589).
    pub window_days: i64,
    /// py `max(60, flow_interval)`.
    pub interval_secs: u64,
}

impl Default for FlowPassConfig {
    fn default() -> Self {
        Self {
            source_threshold: 0.05,
            sink_threshold: -0.05,
            window_days: 7,
            interval_secs: FLOW_DEFAULT_INTERVAL_SECONDS,
        }
    }
}

/// The flow-analysis loop's concrete observer pass.
pub struct FlowAnalysisPass {
    socket_path: PathBuf,
    observer: ObserverHandle,
    boot_id: String,
    cfg: FlowPassConfig,
}

impl FlowAnalysisPass {
    pub fn new(
        socket_path: PathBuf,
        observer: ObserverHandle,
        boot_id: String,
        cfg: FlowPassConfig,
    ) -> Self {
        Self {
            socket_path,
            observer,
            boot_id,
            cfg,
        }
    }

    pub fn interval_secs(&self) -> u64 {
        self.cfg.interval_secs.max(FLOW_MIN_INTERVAL_SECONDS)
    }

    /// Assemble every channel's history from the Rust-owned stores.
    ///
    /// A failure in ANY of the three reads fails the whole assembly: a
    /// partial history would silently reset the filters of whichever
    /// channels the failed read covered, which looks identical to a
    /// genuinely new channel.
    async fn history(&self, now: i64) -> Result<BTreeMap<String, ChannelHistory>, String> {
        let buckets = self
            .observer
            .daily_flow_buckets(now, self.cfg.window_days)
            .await
            .map_err(|e| format!("daily flow buckets unreadable: {e:#}"))?;
        let kalman = self
            .observer
            .kalman_states()
            .await
            .map_err(|e| format!("kalman states unreadable: {e:#}"))?;
        let previous = self
            .observer
            .channel_flow_states()
            .await
            .map_err(|e| format!("channel flow states unreadable: {e:#}"))?;

        let mut history: BTreeMap<String, ChannelHistory> = BTreeMap::new();
        for (scid, daily) in buckets {
            let entry = history.entry(scid).or_default();
            // The SAME bucket vector feeds both frozen kernels: the
            // volatility/decay path reads net flow as f64, the EMA path
            // reads the counted sats form.
            entry.daily = daily
                .iter()
                .map(|b| DailyBucket {
                    in_: b.in_sats as f64,
                    out: b.out_sats as f64,
                })
                .collect();
            entry.ema = daily
                .iter()
                .map(|b| EmaBucket {
                    in_sats: b.in_sats,
                    out_sats: b.out_sats,
                    count: b.count,
                    last_ts: b.last_ts,
                })
                .collect();
        }
        for (scid, encoded, _updated_at) in kalman {
            history.entry(scid).or_default().kalman = Some(decode_kalman(&encoded));
        }
        for row in previous {
            let entry = history.entry(row.scid).or_default();
            entry.previous_state = Some(row.flow_state);
            entry.previous_ratio = row.flow_ratio;
            entry.previous_ratio_at = row.updated_at;
        }
        Ok(history)
    }
}

/// `KalmanFlowState` lives in the FROZEN analytics crate and derives no
/// serde traits, so the JSON encoding is written out field by field
/// through the canonical `to_dict`/`from_dict` pair. Doing it by hand
/// somewhere else would create a second encoding authority; dropping a
/// field would silently reset that part of the filter every pass while
/// still looking like it persisted.
fn encode_kalman(state: &KalmanFlowState) -> Value {
    let d = state.to_dict();
    json!({
        "flow_ratio": d.flow_ratio,
        "flow_velocity": d.flow_velocity,
        "variance_ratio": d.variance_ratio,
        "variance_velocity": d.variance_velocity,
        "covariance": d.covariance,
        "last_update": d.last_update,
        "innovation_variance": d.innovation_variance,
        "last_innovation": d.last_innovation,
        "observation_count": d.observation_count,
    })
}

/// The inverse. An absent or non-numeric key stays `None`, which
/// `from_dict` resolves to that field's documented default — deliberately
/// NOT a blanket zero (`variance_ratio`'s default is the initial variance,
/// and zeroing it would claim perfect certainty on a fresh filter).
fn decode_kalman(encoded: &Value) -> KalmanFlowState {
    let f = |key: &str| encoded.get(key).and_then(Value::as_f64);
    let i = |key: &str| encoded.get(key).and_then(Value::as_i64);
    KalmanFlowState::from_dict(&KalmanStateDict {
        flow_ratio: f("flow_ratio"),
        flow_velocity: f("flow_velocity"),
        variance_ratio: f("variance_ratio"),
        variance_velocity: f("variance_velocity"),
        covariance: f("covariance"),
        last_update: i("last_update"),
        innovation_variance: f("innovation_variance"),
        last_innovation: f("last_innovation"),
        observation_count: i("observation_count"),
    })
}

impl ObserverPass for FlowAnalysisPass {
    fn run<'a>(
        &'a self,
        _key: RequestKey,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let now = crate::now_unix();
            let peer_channels_raw =
                read_rpc(&self.socket_path, "listpeerchannels", json!({})).await;
            let history = self.history(now).await;

            let result = run_flow_pass(FlowDeps {
                peer_channels_raw,
                history,
                source_threshold: self.cfg.source_threshold,
                sink_threshold: self.cfg.sink_threshold,
                now,
                boot_id: &self.boot_id,
            })
            .map_err(|refusal| {
                anyhow::anyhow!("{}: {refusal:?}", refusal.code()).context("flow analysis pass")
            })?;

            // F71-R18: all-or-nothing. See the module doc for why a
            // per-row loop here is undetectably wrong on partial failure.
            let kalman = result
                .kalman
                .iter()
                .map(|(scid, state)| (scid.clone(), encode_kalman(state)))
                .collect();
            self.observer
                .persist_flow_pass(result.states, kalman, now)
                .await
                .context("persist flow pass")?;
            Ok(())
        })
    }
}

// =====================================================================
// StartupSnapshotPass
// =====================================================================

/// The one-shot startup-snapshot loop's concrete observer pass.
///
/// `now_override` exists because this pass's whole contract is a time
/// window ("no connection event in the last hour"), and a test that
/// cannot pin `now` cannot distinguish a correct window from no window at
/// all.
pub struct StartupSnapshotPass {
    socket_path: PathBuf,
    observer: ObserverHandle,
    now_override: Option<i64>,
}

impl StartupSnapshotPass {
    pub fn new(socket_path: PathBuf, observer: ObserverHandle, now_override: i64) -> Self {
        Self {
            socket_path,
            observer,
            now_override: Some(now_override),
        }
    }

    /// Production constructor: reads the clock at pass time.
    pub fn live(socket_path: PathBuf, observer: ObserverHandle) -> Self {
        Self {
            socket_path,
            observer,
            now_override: None,
        }
    }

    fn now(&self) -> i64 {
        self.now_override.unwrap_or_else(crate::now_unix)
    }
}

impl ObserverPass for StartupSnapshotPass {
    fn run<'a>(
        &'a self,
        _key: RequestKey,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let now = self.now();
            let peers_raw = read_rpc(&self.socket_path, "listpeers", json!({})).await;
            let recent: Result<BTreeSet<String>, String> = self
                .observer
                .peers_with_recent_connection_history(now - RECENT_HISTORY_WINDOW_SECONDS)
                .await
                .map_err(|e| format!("recent connection history unreadable: {e:#}"));

            // The planner BORROWS the set, so `recent` has to stay owned
            // here for the duration of the call.
            let plan = plan_startup_snapshot(SnapshotDeps {
                peers_raw,
                peers_with_recent_history: recent.as_ref().map_err(|e| e.clone()),
                now,
            })
            .map_err(|refusal| {
                anyhow::anyhow!("{}: {refusal:?}", refusal.code()).context("startup snapshot pass")
            })?;

            for peer_id in &plan.record_peer_ids {
                self.observer
                    .insert_peer_connection_event(
                        peer_id.clone(),
                        SNAPSHOT_EVENT_TYPE.to_string(),
                        plan.recorded_at,
                    )
                    .await
                    .with_context(|| format!("record startup snapshot event for {peer_id}"))?;
            }
            Ok(())
        })
    }
}

// =====================================================================
// FinancialSnapshotPass
// =====================================================================

/// Where one financial snapshot's two required inputs come from.
enum FinancialSources {
    /// Production: TLV from `listfunds` through the canonical
    /// [`crate::econ_evidence::total_liquidating_value`] — which is what
    /// gives that module a real, non-test consumer — and lifetime stats
    /// from the canonical [`revops_db::queries::lifetime_stats`].
    Live {
        socket_path: PathBuf,
        db: Option<revops_db::actor::DbHandle>,
    },
    /// Injected evidence, so the refusal and arithmetic paths are
    /// drivable without a node.
    #[cfg(test)]
    Fixed {
        tlv_raw: Result<Value, String>,
        lifetime: Result<LifetimeStats, String>,
        now: i64,
    },
}

/// The financial-snapshot loop's concrete observer pass.
pub struct FinancialSnapshotPass {
    observer: ObserverHandle,
    sources: FinancialSources,
}

impl FinancialSnapshotPass {
    pub fn live(
        observer: ObserverHandle,
        socket_path: PathBuf,
        db: Option<revops_db::actor::DbHandle>,
    ) -> Self {
        Self {
            observer,
            sources: FinancialSources::Live { socket_path, db },
        }
    }

    #[cfg(test)]
    pub fn for_tests(
        observer: ObserverHandle,
        tlv_raw: Result<Value, String>,
        lifetime: Result<LifetimeStats, String>,
        now: i64,
    ) -> Self {
        Self {
            observer,
            sources: FinancialSources::Fixed {
                tlv_raw,
                lifetime,
                now,
            },
        }
    }

    async fn evidence(&self) -> (Result<Value, String>, Result<LifetimeStats, String>, i64) {
        match &self.sources {
            FinancialSources::Live { socket_path, db } => {
                let now = crate::now_unix();
                // The canonical TLV producer refuses on a malformed but
                // successful `listfunds`; that refusal is carried through
                // as the failed source rather than flattened to a zero.
                let tlv = match read_rpc(socket_path, "listfunds", json!({})).await {
                    Ok(funds) => crate::econ_evidence::total_liquidating_value(
                        crate::econ_evidence::TlvSources {
                            listfunds: Ok(funds),
                        },
                    )
                    .map(|summary| {
                        json!({
                            "local_balance_sats": summary.local_balance_sats,
                            "remote_balance_sats": summary.remote_balance_sats,
                            "onchain_sats": summary.onchain_sats,
                            "channel_count": summary.channel_count,
                        })
                    })
                    .map_err(|refusal| format!("{}: {}", refusal.code(), refusal.detail())),
                    Err(error) => Err(error),
                };
                let lifetime = match db {
                    Some(handle) => revops_db::queries::lifetime_stats(handle, now)
                        .await
                        .map(|stats| LifetimeStats {
                            total_revenue_msat: stats.total_revenue_msat,
                            total_rebalance_cost_sats: stats.total_rebalance_cost_sats,
                        })
                        .map_err(|e| format!("lifetime stats unreadable: {e:#}")),
                    None => Err(
                        "lifetime stats unavailable: no production database is attached"
                            .to_string(),
                    ),
                };
                (tlv, lifetime, now)
            }
            #[cfg(test)]
            FinancialSources::Fixed {
                tlv_raw,
                lifetime,
                now,
            } => (tlv_raw.clone(), lifetime.clone(), *now),
        }
    }
}

impl ObserverPass for FinancialSnapshotPass {
    fn run<'a>(
        &'a self,
        _key: RequestKey,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let (tlv_raw, lifetime, now) = self.evidence().await;
            let row = plan_financial_snapshot(FinancialDeps {
                tlv_raw,
                lifetime,
                now,
            })
            .map_err(|refusal| {
                anyhow::anyhow!("{}: {refusal:?}", refusal.code())
                    .context("financial snapshot pass")
            })?;
            self.observer
                .insert_financial_snapshot(row)
                .await
                .context("persist financial snapshot")?;
            Ok(())
        })
    }
}
