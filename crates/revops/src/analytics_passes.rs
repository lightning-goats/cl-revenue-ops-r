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
use revops_analytics::flow::{EmaBucket, HourlyHistogramBucket, TemporalProfile};
use revops_analytics::kalman::{DailyBucket, KalmanFlowState, KalmanStateDict, NetFlowEntry};
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

/// py `_compute_raw_kalman_observation` filters raw entries to the last
/// 86400s, so the fetch window matches exactly (flow_analysis.py:1402-1408).
const RAW_OBSERVATION_WINDOW_SECONDS: i64 = 86_400;

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
    /// py `htlc_congestion_threshold` (config.py:743).
    pub htlc_congestion_threshold: f64,
    /// py `max(60, flow_interval)`.
    pub interval_secs: u64,
}

impl Default for FlowPassConfig {
    fn default() -> Self {
        Self {
            source_threshold: 0.05,
            sink_threshold: -0.05,
            window_days: 7,
            htlc_congestion_threshold: 0.8,
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
        // F71-R19: the raw 24h per-forward window the Kalman filter
        // actually observes. py fetches exactly 24h here because the
        // consumer filters to <= 86400s anyway.
        let raw = self
            .observer
            .continuous_net_flow(now - RAW_OBSERVATION_WINDOW_SECONDS)
            .await
            .map_err(|e| format!("continuous net flow unreadable: {e:#}"))?;

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
        for (scid, rows) in raw {
            history.entry(scid).or_default().raw_entries = rows
                .into_iter()
                .map(|r| NetFlowEntry {
                    timestamp: r.timestamp as f64,
                    net_msat: r.net_msat,
                })
                .collect();
        }
        // F71-R20: the DTS posterior variance py digs out of each
        // channel's `v2_state_json` before deciding whether to widen the
        // flow thresholds. Nested-first, flat fallback, matching py's
        // `(v2.get('fee_state') or {}).get('thompson_state') or
        // v2.get('thompson_state', {})`.
        let fee_state = self
            .observer
            .load_latest_fee_state()
            .await
            .map_err(|e| format!("fee strategy state unreadable: {e:#}"))?;
        for row in fee_state.rows {
            let entry = history.entry(row.channel_id).or_default();
            if let Some(variance) = posterior_variance(&row.v2_state_json) {
                entry.posterior_variance = Some(variance);
            }
            // F71-R25b: the temporal profile's `dominant_bucket` is owned
            // by the fee controller's size profiling, not by this pass.
            entry.dominant_bucket_override = dominant_size_bucket(&row.v2_state_json);
        }
        // F71-R25: the hour-of-day histogram and the persisted temporal
        // profile the frozen kernel EMA-blends it into.
        let histograms = self
            .observer
            .hourly_flow_histogram(now, revops_db::analytics::TEMPORAL_HISTOGRAM_WINDOW_DAYS)
            .await
            .map_err(|e| format!("hourly flow histogram unreadable: {e:#}"))?;
        for (scid, buckets) in histograms {
            let mut converted = [HourlyHistogramBucket::default(); 24];
            for (slot, bucket) in buckets.iter().enumerate() {
                converted[slot] = HourlyHistogramBucket {
                    out_sats: bucket.out_sats as f64,
                    in_sats: bucket.in_sats as f64,
                    count: bucket.count as f64,
                };
            }
            history.entry(scid).or_default().hourly_histogram = Some(converted);
        }
        let profiles = self
            .observer
            .temporal_profiles()
            .await
            .map_err(|e| format!("temporal profiles unreadable: {e:#}"))?;
        for (scid, encoded, _updated_at) in profiles {
            let profile = decode_temporal(&scid, &encoded)?;
            history.entry(scid).or_default().temporal_profile = Some(profile);
        }
        for (scid, encoded, _updated_at) in kalman {
            history.entry(scid).or_default().kalman = Some(decode_kalman(&encoded));
        }
        for row in previous {
            let entry = history.entry(row.scid).or_default();
            entry.previous_state = Some(row.flow_state);
            entry.previous_ratio = row.flow_ratio;
            entry.previous_ratio_at = row.updated_at;
            // F71-R21: the balance classifier's veto reads the PREVIOUS
            // cycle's Kalman estimate, which is why R20's separate column
            // had to exist before this could be sourced at all.
            entry.previous_kalman_ratio = row.kalman_flow_ratio;
        }
        Ok(history)
    }
}

/// Dig `posterior_variance` out of a channel's `v2_state_json`.
///
/// Py wraps the whole dig in a bare `try/except` and falls through to "no
/// widening" on ANY failure, so an unparseable envelope returning `None`
/// here is faithful rather than lossy. A missing key is py's `10000`
/// default, which its `> 10000` test then rejects -- identical in effect
/// to `None`, so it is not synthesized.
fn posterior_variance(v2_state_json: &str) -> Option<f64> {
    let v2: Value = serde_json::from_str(v2_state_json).ok()?;
    let thompson = v2
        .get("fee_state")
        .and_then(|f| f.get("thompson_state"))
        .or_else(|| v2.get("thompson_state"))?;
    thompson.get("posterior_variance").and_then(Value::as_f64)
}

/// Pick the size bucket with the highest `revenue_share` from a channel's
/// `v2_state_json` (py flow_analysis.py:1713-1723).
///
/// `None` reproduces py's `except: pass` -- size profiling unavailable, so
/// the stored profile keeps whatever label it already had. That is
/// distinct from `Some("unknown")`, which is py's own answer when the
/// buckets ARE present but none carries a positive share: the fee
/// controller looked and could not name a dominant bucket.
pub(crate) fn dominant_size_bucket(v2_state_json: &str) -> Option<String> {
    let v2: Value = serde_json::from_str(v2_state_json).ok()?;
    let buckets = v2.get("size_buckets")?.as_object()?;
    let mut max_share = 0.0f64;
    let mut dominant = "unknown".to_string();
    for (label, data) in buckets {
        let share = data
            .get("revenue_share")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        if share > max_share {
            max_share = share;
            dominant = label.clone();
        }
    }
    Some(dominant)
}

/// `TemporalProfile` is in the same frozen crate as `KalmanFlowState` and
/// likewise derives no serde traits, so it is encoded field by field. The
/// derived fields (`peak_hours`, `quiet_hours`, `burstiness`,
/// `diurnal_strength`) are RECOMPUTED by the kernel from the hourly arrays
/// on every update, so they are stored for readers but never trusted on
/// the way back in.
fn encode_temporal(p: &TemporalProfile) -> Value {
    json!({
        "hourly_out": p.hourly_out.to_vec(),
        "hourly_in": p.hourly_in.to_vec(),
        "hourly_count": p.hourly_count.to_vec(),
        "peak_hours": p.peak_hours,
        "quiet_hours": p.quiet_hours,
        "burstiness": p.burstiness,
        "diurnal_strength": p.diurnal_strength,
        "dominant_bucket": p.dominant_bucket,
        "observation_days": p.observation_days,
        "last_observation_day": p.last_observation_day,
        "last_updated": p.last_updated,
    })
}

/// Decode a PERSISTED temporal profile, refusing anything malformed.
///
/// F71-R25a. My first draft defaulted a corrupt, short, or non-numeric
/// stored profile into a fresh zero profile, with a comment calling that
/// "the honest reading". It is not. An all-zero `hourly_out` is exactly
/// what the frozen kernel tests for its `is_first` branch, so a corrupt
/// row does not degrade gracefully — it makes the kernel DISCARD the
/// channel's accumulated history, take the new histogram verbatim, and
/// leave `observation_days` frozen, silently weakening the graduation
/// check that gates predictive pre-positioning and demand-based sizing.
///
/// Note the deliberate asymmetry with [`decode_kalman`], which IS
/// permissive: `KalmanFlowState::from_dict` carries documented,
/// audit-fixed semantics (I-7) that this port mirrors on purpose.
/// `TemporalProfile::from_dict` has no such contract — py just does
/// `d.get(k, default)[:24]`, truncating without ever checking length.
///
/// An ABSENT row is NOT malformed: a channel with no stored profile has
/// genuinely never been observed, and `TemporalProfile::default()` is the
/// correct, meaningful answer for it.
fn decode_temporal(scid: &str, encoded: &Value) -> Result<TemporalProfile, String> {
    let hours = |key: &str| -> Result<[f64; 24], String> {
        let values = encoded
            .get(key)
            .and_then(Value::as_array)
            .ok_or_else(|| format!("temporal profile for {scid} has no {key} array"))?;
        if values.len() != 24 {
            return Err(format!(
                "temporal profile for {scid} has {} {key} buckets, expected exactly 24",
                values.len()
            ));
        }
        let mut out = [0.0f64; 24];
        for (slot, value) in values.iter().enumerate() {
            out[slot] = value.as_f64().ok_or_else(|| {
                format!("temporal profile for {scid} has a non-numeric {key}[{slot}]: {value}")
            })?;
        }
        Ok(out)
    };
    let counter = |key: &str| -> Result<i64, String> {
        encoded
            .get(key)
            .ok_or_else(|| format!("temporal profile for {scid} has no {key}"))?
            .as_i64()
            .ok_or_else(|| format!("temporal profile for {scid} has a non-integer {key}"))
    };

    let mut profile = TemporalProfile {
        hourly_out: hours("hourly_out")?,
        hourly_in: hours("hourly_in")?,
        hourly_count: hours("hourly_count")?,
        ..Default::default()
    };
    profile.observation_days = counter("observation_days")?;
    profile.last_observation_day = counter("last_observation_day")?;
    profile.last_updated = counter("last_updated")?;
    profile.dominant_bucket = encoded
        .get("dominant_bucket")
        .ok_or_else(|| format!("temporal profile for {scid} has no dominant_bucket"))?
        .as_str()
        .ok_or_else(|| format!("temporal profile for {scid} has a non-string dominant_bucket"))?
        .to_string();
    Ok(profile)
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
                flow_window_days: self.cfg.window_days,
                htlc_congestion_threshold: self.cfg.htlc_congestion_threshold,
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
            let temporal = result
                .temporal
                .iter()
                .map(|(scid, profile)| (scid.clone(), encode_temporal(profile)))
                .collect();
            self.observer
                .persist_flow_pass(result.states, kalman, temporal, result.observed_scids, now)
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
