//! Task 67b slice 2: assemble `ChannelProfitability` per channel.
//!
//! The classifier and the profitability type are FROZEN
//! (`revops_analytics::profitability`). This module only assembles their
//! inputs from the Rust-owned reads, using Python's exact arithmetic
//! (modules/profitability_analyzer.py:795-880):
//!
//! - `net_profit = total_contribution - total_cost`, where contribution is
//!   direct + sourced (per-channel VALUATION legitimately counts both;
//!   only FLEET revenue must not — see `queries::PerChannelRevenue`).
//! - `roi = net_profit / total_cost` when a cost exists. With NO recorded
//!   cost Python does not divide-guard to zero: a channel earning with no
//!   cost gets a synthetic `1.0` ("free money"), and only a channel with
//!   no contribution at all falls back to return-on-capacity.
//! - Marginal ROI is the 30-day window over ONGOING rebalance cost with no
//!   sunk open cost. Conflating it with the all-time figure flips winners
//!   into losers.
//!
//! A missing `opened_at` REFUSES rather than defaulting `days_open` to 0 —
//! zero would make every staleness branch read as "too new to judge",
//! which is a silent misclassification rather than a visible gap.

use std::collections::HashMap;

use serde_json::json;

use revops_analytics::profitability::{
    classify_channel, ChannelCosts, ChannelProfitability, ChannelRevenue, ClassifyEvidence,
    DiagStats,
};
use revops_db::queries::{PerChannelCosts, PerChannelRevenue};

/// One channel's assembled inputs.
#[derive(Debug, Clone)]
pub struct ChannelInput {
    pub scid: String,
    pub revenue_all_time: PerChannelRevenue,
    pub revenue_30d: PerChannelRevenue,
    pub costs: PerChannelCosts,
    /// `"local"` or `"remote"` (from the live channel snapshot).
    pub opener: String,
    pub last_routed: Option<i64>,
    pub diag_attempt_count: i64,
    pub diag_last_success_time: i64,
    pub posterior_variance: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfitabilityRefusal {
    OpenTimestampMissing(String),
}

impl ProfitabilityRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::OpenTimestampMissing(_) => "profitability_open_timestamp_missing",
        }
    }
}

fn to_revenue(scid: &str, r: &PerChannelRevenue) -> ChannelRevenue {
    ChannelRevenue {
        channel_id: scid.to_string(),
        fees_earned_msat: r.fees_earned_msat,
        volume_routed_msat: r.volume_routed_msat,
        forward_count: r.forward_count,
        sourced_volume_msat: r.sourced_volume_msat,
        sourced_fee_contribution_msat: r.sourced_fee_contribution_msat,
        sourced_forward_count: r.sourced_forward_count,
    }
}

/// Assemble one channel and run the frozen classifier.
pub fn assemble_channel_profitability(
    input: ChannelInput,
    now: i64,
) -> Result<ChannelProfitability, ProfitabilityRefusal> {
    if input.costs.opened_at <= 0 {
        return Err(ProfitabilityRefusal::OpenTimestampMissing(format!(
            "channel {} has no opened_at; days_open cannot be derived and defaulting it \
             to 0 would make every staleness branch read as 'too new to judge'",
            input.scid
        )));
    }

    let costs = ChannelCosts {
        channel_id: input.scid.clone(),
        peer_id: input.costs.peer_id.clone(),
        open_cost_sats: input.costs.open_cost_sats,
        rebalance_cost_sats: input.costs.rebalance_cost_sats,
        effective_rebalance_cost_sats: input.costs.rebalance_cost_sats,
    };
    let revenue = to_revenue(&input.scid, &input.revenue_all_time);
    let revenue_30d = to_revenue(&input.scid, &input.revenue_30d);

    let total_cost = costs.total_cost_sats();
    let total_contribution = revenue.total_contribution_sats();
    let net_profit_sats = total_contribution - total_cost;

    // py:803-816 -- NOT a divide guard.
    let roi = if total_cost > 0 {
        net_profit_sats as f64 / total_cost as f64
    } else if total_contribution > 0 {
        1.0
    } else {
        total_contribution as f64 / input.costs.capacity_sats.max(1) as f64
    };

    // py:818-824 -- physical throughput, not double-counted.
    let total_volume = revenue
        .volume_routed_sats()
        .max(revenue.sourced_volume_sats());
    let (cost_per_sat_routed, fee_per_sat_routed) = if total_volume > 0 {
        (
            total_cost as f64 / total_volume as f64,
            total_contribution as f64 / total_volume as f64,
        )
    } else {
        (0.0, 0.0)
    };

    let days_open = (now - input.costs.opened_at).div_euclid(86_400);
    let contribution_30d_msat = revenue_30d.total_contribution_msat();
    let marginal_profit_30d_sats =
        revenue_30d.total_contribution_sats() - input.costs.rebalance_cost_30d_sats;

    let diag = DiagStats {
        attempt_count: input.diag_attempt_count,
        last_success_time: input.diag_last_success_time,
    };
    let classification = classify_channel(
        roi,
        net_profit_sats,
        input.last_routed,
        days_open,
        revenue.total_forward_count(),
        &ClassifyEvidence {
            now,
            diag_stats: Some(&diag),
            posterior_variance: input.posterior_variance,
            contribution_30d_msat: Some(contribution_30d_msat),
        },
    );

    Ok(ChannelProfitability {
        channel_id: input.scid.clone(),
        peer_id: input.costs.peer_id.clone(),
        capacity_sats: input.costs.capacity_sats,
        costs,
        revenue,
        net_profit_sats,
        roi_percent: roi * 100.0,
        classification,
        cost_per_sat_routed,
        fee_per_sat_routed,
        days_open,
        last_routed: input.last_routed,
        marginal_profit_30d_sats,
        rebalance_cost_30d_sats: input.costs.rebalance_cost_30d_sats,
        opener: input.opener,
        contribution_30d_msat,
        fees_earned_30d_msat: revenue_30d.fees_earned_msat,
        sourced_fee_30d_msat: revenue_30d.sourced_fee_contribution_msat,
        forward_count_30d: revenue_30d.forward_count,
        sourced_forward_count_30d: revenue_30d.sourced_forward_count,
        window_30d_available: true,
    })
}

/// One fleet pass: assembled channels plus the ones skipped and WHY.
#[derive(Debug, Default)]
pub struct FleetProfitability {
    pub profitability: HashMap<String, ChannelProfitability>,
    /// (scid, reason) -- surfaced, never silently dropped.
    pub skipped: Vec<(String, String)>,
}

/// py's trailing window for marginal ROI and its
/// `get_diagnostic_rebalance_stats(scid, days=14)` default
/// (database.py:2787). Constants rather than options because Python does
/// not expose either as one.
pub const PROFITABILITY_WINDOW_DAYS: i64 = 30;
pub const DIAGNOSTIC_WINDOW_DAYS: i64 = 14;

/// The stores one fleet profitability pass reads, and the already-fetched
/// channel snapshot it is told about.
///
/// `channels` is passed IN rather than fetched here: the opener must come
/// from the same bounded snapshot the caller already took, not from a
/// second query that could disagree with it.
pub struct ProfitabilitySources<'a> {
    pub production_db: &'a revops_db::actor::DbHandle,
    pub observer: &'a revops_db::owner::ObserverHandle,
    pub channels: &'a [serde_json::Value],
    pub now: i64,
}

/// py `_scid_aliases`, applied to every key so the three sources agree.
fn normalize_scid(scid: &str) -> String {
    scid.replace(':', "x")
}

/// The opener of each channel in an already-fetched `listpeerchannels`
/// snapshot. Channels without a `short_channel_id` (still opening) or
/// without an `opener` are simply absent -- absence is what makes the
/// channel skip, and inventing `"local"` here is the thing C71-25 removes.
pub fn openers_from_channels(channels: &[serde_json::Value]) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for channel in channels {
        let Some(scid) = channel.get("short_channel_id").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(opener) = channel.get("opener").and_then(|v| v.as_str()) else {
            continue;
        };
        out.insert(normalize_scid(scid), opener.to_string());
    }
    out
}

/// Same timeout every other read-only CLN call in this port uses.
const RPC_TIMEOUT_SECONDS: u64 = 30;

/// One bounded, read-only CLN call. Fresh connection per call, same
/// timeout every other read path in this port uses. Errors are stringly
/// typed because they become typed refusals at the RPC boundary, naming
/// the source that could not be reached.
pub async fn fetch_read_rpc(
    socket_path: &std::path::Path,
    method: &'static str,
) -> Result<serde_json::Value, String> {
    let call = async {
        let mut rpc = cln_rpc::ClnRpc::new(socket_path).await.map_err(|e| {
            anyhow::anyhow!(
                "connect lightning-rpc socket {}: {e}",
                socket_path.display()
            )
        })?;
        rpc.call_raw::<serde_json::Value, serde_json::Value>(method, &json!({}))
            .await
            .map_err(|e| anyhow::anyhow!("{method} RPC error: {e}"))
    };
    revops_rpc::call_with_timeout(method, RPC_TIMEOUT_SECONDS, call)
        .await
        .map_err(|error| format!("{error}"))
}

/// One fresh, bounded `listpeerchannels`.
///
/// Separate from [`gather_profitability`] on purpose: the caller takes the
/// snapshot ONCE and hands that same value in, so the opener the verdict
/// is built from is the opener the caller saw. Fetching inside the
/// producer would let a second query disagree with the first, and nothing
/// downstream could tell.
///
/// A failed call is `Err`, never an empty channel list -- an empty list
/// would skip every channel for "no opener", which reads like a fleet of
/// unevaluable channels rather than an unreachable node.
pub async fn fetch_channel_snapshot(
    socket_path: &std::path::Path,
) -> Result<Vec<serde_json::Value>, String> {
    let reply = fetch_read_rpc(socket_path, "listpeerchannels").await?;
    reply
        .get("channels")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .ok_or_else(|| "listpeerchannels reply carries no channels array".to_string())
}

/// One fleet profitability pass: ONE production-database await, ONE
/// observer-store await, and the caller's already-fetched channel
/// snapshot.
///
/// It never calls `per_channel_revenue`, `per_channel_costs` or
/// `channel_history`. Those are separate actor turns against a database
/// Python writes concurrently under WAL, and composing them would produce
/// a fleet state that existed at no instant (C71-21).
///
/// Store-level failures refuse for the whole fleet. Per-channel evidence
/// problems -- a corrupt fee posterior, a channel absent from the snapshot
/// -- skip that channel WITH a reason, so one bad row cannot blank the
/// whole surface and cannot pass unnoticed either.
pub async fn gather_profitability(
    sources: ProfitabilitySources<'_>,
) -> Result<FleetProfitability, crate::profitability_evidence::ProfitabilityEvidenceRefusal> {
    use crate::profitability_evidence::{
        channel_evidence, posterior_variance_from_v2_json, ConsultedSources,
        ProfitabilityEvidenceRefusal,
    };

    let snapshot = sources
        .production_db
        .profitability_snapshot(
            sources.now,
            PROFITABILITY_WINDOW_DAYS,
            DIAGNOSTIC_WINDOW_DAYS,
        )
        .await
        .map_err(|error| ProfitabilityEvidenceRefusal::SnapshotUnavailable {
            detail: format!("{error:#}"),
        })?;

    let fee_state = sources
        .observer
        .load_latest_fee_state()
        .await
        .map_err(|error| ProfitabilityEvidenceRefusal::FeeStoreUnavailable {
            detail: format!("{error:#}"),
        })?;
    let posteriors: HashMap<String, Result<Option<f64>, String>> = fee_state
        .rows
        .iter()
        .map(|row| {
            (
                normalize_scid(&row.channel_id),
                posterior_variance_from_v2_json(&row.v2_state_json),
            )
        })
        .collect();

    let openers = openers_from_channels(sources.channels);

    let mut evidence = HashMap::new();
    let mut refused: Vec<(String, String)> = Vec::new();
    let mut scids: Vec<&String> = snapshot.costs.keys().collect();
    scids.sort();
    for scid in scids {
        let history = snapshot.history.get(scid);
        let consulted = ConsultedSources {
            // The fleet snapshot RAN; a channel absent from it genuinely
            // has no rows, which is Python's own no-row answer.
            last_routed: Ok(history.and_then(|h| h.last_routed)),
            diag: Ok(history.map(|h| h.diag)),
            posterior_variance: posteriors.get(scid).cloned().unwrap_or(Ok(None)),
            opener: Ok(openers.get(scid).cloned()),
        };
        match channel_evidence(scid, consulted) {
            Ok(ev) => {
                evidence.insert(scid.clone(), ev);
            }
            Err(refusal) => {
                refused.push((
                    scid.clone(),
                    format!("{}: {}", refusal.code(), refusal.detail()),
                ));
            }
        }
    }

    let mut fleet = assemble_fleet(
        &snapshot.revenue_all_time,
        &snapshot.revenue_30d,
        &snapshot.costs,
        &evidence,
        sources.now,
    );
    // A refused channel is ALSO absent from `evidence`, so the assembler
    // has already skipped it with its generic "no evidence was gathered"
    // reason. Dropping that here is not cosmetic: the generic reason says
    // nothing was looked up, when in fact the lookup ran and one source
    // came back unreadable. Leaving both in would let the vaguer reason
    // mask the actionable one -- the C71-15 precedence trap.
    let refused_scids: std::collections::HashSet<&str> =
        refused.iter().map(|(scid, _)| scid.as_str()).collect();
    fleet
        .skipped
        .retain(|(scid, _)| !refused_scids.contains(scid.as_str()));
    fleet.skipped.extend(refused);
    fleet.skipped.sort();
    Ok(fleet)
}

/// Assemble every channel that has costs, from evidence that was actually
/// looked up.
///
/// C71-25: this function used to invent `opener: "local"`,
/// `last_routed: None`, zeroed diagnostics and no fee posterior for every
/// channel. Three of those coincide with Python's no-row defaults, which
/// is why no test ever caught them -- but Python reached them by RUNNING
/// the query, and this reached them by not asking. The fourth,
/// `opener: "local"`, is not even Python's default in spirit: it asserts
/// this node paid the opening fee, and that figure is reported to the
/// operator, not merely fed to the classifier.
///
/// A channel with no evidence entry is therefore SKIPPED with a reason.
/// Skipping is visible; a fabricated default is not.
pub fn assemble_fleet(
    revenue_all_time: &HashMap<String, PerChannelRevenue>,
    revenue_30d: &HashMap<String, PerChannelRevenue>,
    costs: &HashMap<String, PerChannelCosts>,
    evidence: &HashMap<String, crate::profitability_evidence::ChannelEvidence>,
    now: i64,
) -> FleetProfitability {
    let mut out = FleetProfitability::default();
    let mut scids: Vec<&String> = costs.keys().collect();
    scids.sort();
    for scid in scids {
        let Some(ev) = evidence.get(scid) else {
            out.skipped.push((
                scid.clone(),
                "no profitability evidence was gathered for this channel; \
                 classifying it from defaults would report an unconsulted \
                 channel as never-routed and locally funded"
                    .to_string(),
            ));
            continue;
        };
        let input = ChannelInput {
            scid: scid.clone(),
            revenue_all_time: revenue_all_time.get(scid).cloned().unwrap_or_default(),
            revenue_30d: revenue_30d.get(scid).cloned().unwrap_or_default(),
            costs: costs.get(scid).cloned().unwrap_or_default(),
            opener: ev.opener.clone(),
            last_routed: ev.last_routed,
            diag_attempt_count: ev.diag.attempt_count,
            diag_last_success_time: ev.diag.last_success_time,
            posterior_variance: ev.posterior_variance,
        };
        match assemble_channel_profitability(input, now) {
            Ok(p) => {
                out.profitability.insert(scid.clone(), p);
            }
            Err(refusal) => match refusal {
                ProfitabilityRefusal::OpenTimestampMissing(detail) => {
                    out.skipped.push((scid.clone(), detail));
                }
            },
        }
    }
    out
}
