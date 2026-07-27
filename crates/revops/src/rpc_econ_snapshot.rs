//! Pure response builder for `revenue-r-econ-snapshot`.
//!
//! Port of `revenue_econ_snapshot` / `_assemble_econ_snapshot`
//! (cl-revenue-ops.py:6019-6089) and `EconShadow.build_snapshot_preview`
//! (modules/econ_shadow.py:457-520). Assembles a full
//! [`revops_econ::snapshot::EconomicSnapshot`] from already-fetched
//! channel/profitability/budget data using the already-ported
//! `revops_econ::snapshot::build_channel_snapshot` per-channel mapper and
//! `revops_analytics::profitability::ChannelProfitability::role_30d` for
//! the role classification -- this is a genuine assembly port, not a
//! stub, because every primitive it needs already exists in
//! `revops-econ`/`revops-analytics`.
//!
//! Per Python's own doc comment on `build_snapshot_preview`
//! ("placeholder fields are DECLARED in the returned approximations
//! list"), four things stay approximated exactly as upstream declares:
//! `lifecycle` is always `"PRODUCTIVE"` (the lifecycle model is a future
//! workstream), `confidence_micro` is always `0` (flow confidence isn't
//! wired into this assembly), `onchain_confirmed_msat`/`reserved_msat`/
//! `sourced_volume_msat` are always `0`, and `protections` is always
//! empty (policy tags aren't wired into this assembly). These are
//! Python's OWN documented approximations, not gaps this port introduces.

use revops_analytics::profitability::ChannelProfitability;
use revops_econ::snapshot::{
    build_channel_snapshot, BudgetState, EconomicSnapshot, NodeState, ProfEvidence,
};
use revops_econ::types::{Msat, UnixTime};
use serde_json::{json, Value};
use std::collections::HashMap;

/// Already-fetched unified-budget figures for the preview's `daily_budget`
/// block (`_assemble_econ_snapshot`'s `budget` dict, cl-revenue-ops.py:
/// 6038-6046).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BudgetPreviewInputs {
    pub cap_sats: i64,
    pub reserved_sats: i64,
    pub spent_sats: i64,
}

/// `ChannelProfitability` -> `ProfEvidence`, the flattened evidence shape
/// `build_channel_snapshot` consumes (see that function's doc comment).
fn prof_evidence(p: &ChannelProfitability) -> ProfEvidence {
    ProfEvidence {
        fees_earned_msat: p.revenue.fees_earned_msat,
        sourced_fee_contribution_msat: p.revenue.sourced_fee_contribution_msat,
        rebalance_cost_sats: p.costs.rebalance_cost_sats,
        open_cost_sats: p.costs.open_cost_sats,
        net_profit_sats: p.net_profit_sats,
        volume_routed_msat: p.revenue.volume_routed_msat,
        forward_count: p.revenue.forward_count,
        sourced_forward_count_30d: p.sourced_forward_count_30d,
    }
}

/// Python `str(channel)[:80]`-equivalent truncation for a skipped-channel
/// approximation note (cl-revenue-ops.py:6042/econ_shadow.py:483-485).
fn truncate80(v: &Value) -> String {
    let s = v.to_string();
    if s.chars().count() <= 80 {
        s
    } else {
        s.chars().take(80).collect()
    }
}

/// Port of `EconShadow.build_snapshot_preview`. `channels` is a
/// listpeerchannels-shaped, already-normalized JSON array (each element
/// needs `short_channel_id`/`peer_id`/`total_msat`/`to_us_msat`, matching
/// `build_channel_snapshot`'s contract); `profitability` maps `scid` ->
/// already-computed `ChannelProfitability` (Python's `profitability.get
/// (scid)`, `None`/missing entries mean "no evidence", same as Python's
/// `.get()` returning `None`).
///
/// Returns `(wire, approximations)`: `wire` is `None` only if the WHOLE
/// assembly fails (mirrors Python's outer `except Exception` in
/// `build_snapshot_preview` -- an invalid `snapshot_id`/window, which
/// cannot actually happen with this function's own inputs, or arithmetic
/// overflow summing channel totals); a single bad channel is skipped and
/// noted in `approximations`, never a whole-snapshot failure (mirrors the
/// per-channel `try/except` in the Python loop).
pub fn assemble_snapshot_preview(
    channels: &[Value],
    profitability: &HashMap<String, ChannelProfitability>,
    budget: &BudgetPreviewInputs,
    now: i64,
    receivable_ratio_target: f64,
) -> (Option<Value>, Vec<String>) {
    let mut approximations = vec![
        "lifecycle=PRODUCTIVE for all channels (lifecycle model is Workstream F5)".to_string(),
        "confidence_micro=0 (flow confidence not wired yet)".to_string(),
        "onchain_confirmed_msat=0, reserved_msat=0, sourced_volume_msat=0 (not wired yet)"
            .to_string(),
        "protections not populated (policy tags not wired yet)".to_string(),
    ];

    let mut channel_snaps = Vec::new();
    for channel in channels {
        let scid = match channel.get("short_channel_id").and_then(Value::as_str) {
            Some(s) => s,
            None => {
                approximations.push(format!(
                    "channel skipped (missing short_channel_id): {}",
                    truncate80(channel)
                ));
                continue;
            }
        };
        let prof = profitability.get(scid);
        let role = prof
            .map(|p| p.role_30d().as_name())
            .filter(|name| revops_econ::snapshot::ROLES.contains(name))
            .unwrap_or("UNKNOWN");
        let evidence = prof.map(prof_evidence);
        match build_channel_snapshot(
            channel,
            evidence.as_ref(),
            None,
            role,
            "PRODUCTIVE",
            Vec::new(),
        ) {
            Ok(snap) => channel_snaps.push(snap),
            Err(e) => {
                approximations.push(format!("channel skipped ({e}): {}", truncate80(channel)))
            }
        }
    }

    let totals = (|| -> Result<(Msat, Msat, Msat), revops_econ::types::EconError> {
        let mut local = Msat::new(0)?;
        let mut remote = Msat::new(0)?;
        let mut capacity = Msat::new(0)?;
        for c in &channel_snaps {
            local = local.add(c.local_msat)?;
            remote = remote.add(c.remote_msat)?;
            capacity = capacity.add(c.capacity_msat)?;
        }
        Ok((local, remote, capacity))
    })();

    let (total_local, total_remote, total_capacity) = match totals {
        Ok(t) => t,
        Err(e) => {
            approximations.push(format!("preview failed: {e}"));
            return (None, approximations);
        }
    };

    let ratio = receivable_ratio_target.clamp(0.0, 1.0);
    let receivable_objective = (total_capacity.value() as f64 * ratio) as i64;

    let build = || -> Result<Value, revops_econ::types::EconError> {
        let node = NodeState {
            total_local_msat: total_local,
            total_remote_msat: total_remote,
            receivable_objective_msat: Msat::new(receivable_objective)?,
            onchain_confirmed_msat: Msat::new(0)?,
            reserved_msat: Msat::new(0)?,
            daily_budget: BudgetState {
                cap_msat: Msat::from_sats(budget.cap_sats.max(0))?,
                reserved_msat: Msat::from_sats(budget.reserved_sats.max(0))?,
                spent_msat: Msat::from_sats(budget.spent_sats.max(0))?,
            },
            pending_operations: Vec::new(),
            external_obligations: Vec::new(),
        };
        let snap = EconomicSnapshot::new(
            format!("preview-{now}"),
            UnixTime::new(now)?,
            30 * 86400,
            node,
            channel_snaps,
        )?;
        Ok(snap.to_wire())
    };

    match build() {
        Ok(wire) => (Some(wire), approximations),
        Err(e) => {
            approximations.push(format!("preview failed: {e}"));
            (None, approximations)
        }
    }
}

/// Which live-data path produced (or failed to produce) `snapshot`,
/// mirroring `revenue_econ_snapshot`'s two failure shapes
/// (cl-revenue-ops.py:6062-6089): the OUTER try/except around
/// `_assemble_econ_snapshot()` catches a channel-READ failure (before
/// assembly even starts) distinctly from a normal (possibly
/// partially-approximated) assembly.
pub enum SnapshotAssembly<'a> {
    /// The live `listpeerchannels`-equivalent read failed before assembly
    /// could start (`_assemble_econ_snapshot`'s own `channels = ...` line
    /// raising, cl-revenue-ops.py:6070-6072).
    ChannelReadFailed(String),
    /// Channels were fetched; assemble normally via
    /// [`assemble_snapshot_preview`].
    Ready {
        channels: &'a [Value],
        profitability: &'a HashMap<String, ChannelProfitability>,
        budget: &'a BudgetPreviewInputs,
        now: i64,
        receivable_ratio_target: f64,
    },
}

/// Port of `revenue_econ_snapshot`. `assembly: None` is only valid when
/// `enabled=false` (the `econ_shadow_enabled` config gate,
/// cl-revenue-ops.py:6067-6069) -- an `enabled=true` call with no
/// assembly plan is treated the same as a channel-read failure rather
/// than fabricating a snapshot.
pub fn build_econ_snapshot(
    enabled: bool,
    assembly: Option<SnapshotAssembly>,
    intents_recorded_total: Option<i64>,
    intents_ledger_total: Option<i64>,
) -> Value {
    if !enabled {
        return json!({
            "enabled": false,
            "hint": "revenue-config set econ_shadow_enabled true",
        });
    }
    let Some(assembly) = assembly else {
        return json!({
            "enabled": true,
            "snapshot": Value::Null,
            "approximations": ["channel read failed: assembly inputs not provided"],
        });
    };
    match assembly {
        SnapshotAssembly::ChannelReadFailed(reason) => json!({
            "enabled": true,
            "snapshot": Value::Null,
            "approximations": [format!("channel read failed: {reason}")],
        }),
        SnapshotAssembly::Ready {
            channels,
            profitability,
            budget,
            now,
            receivable_ratio_target,
        } => {
            let (wire, approximations) = assemble_snapshot_preview(
                channels,
                profitability,
                budget,
                now,
                receivable_ratio_target,
            );
            json!({
                "enabled": true,
                "snapshot": wire,
                "approximations": approximations,
                "intents_recorded_total": intents_recorded_total,
                "intents_ledger_total": intents_ledger_total,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use revops_analytics::profitability::{ChannelCosts, ChannelRevenue, ProfitabilityClass};

    fn raw_channel(scid: &str, peer_id: &str, total_msat: i64, to_us_msat: i64) -> Value {
        json!({
            "short_channel_id": scid,
            "peer_id": peer_id,
            "total_msat": total_msat,
            "to_us_msat": to_us_msat,
            "spendable_msat": to_us_msat,
            "receivable_msat": total_msat - to_us_msat,
        })
    }

    fn prof(
        channel_id: &str,
        forward_count: i64,
        sourced_forward_count: i64,
    ) -> ChannelProfitability {
        ChannelProfitability {
            channel_id: channel_id.to_string(),
            peer_id: String::new(),
            capacity_sats: 0,
            costs: ChannelCosts {
                channel_id: channel_id.to_string(),
                peer_id: String::new(),
                open_cost_sats: 100,
                rebalance_cost_sats: 50,
                effective_rebalance_cost_sats: 50,
            },
            revenue: ChannelRevenue {
                channel_id: channel_id.to_string(),
                fees_earned_msat: 10_000,
                volume_routed_msat: 1_000_000,
                forward_count,
                sourced_volume_msat: 0,
                sourced_fee_contribution_msat: 0,
                sourced_forward_count,
            },
            net_profit_sats: 50,
            roi_percent: 10.0,
            classification: ProfitabilityClass::Profitable,
            cost_per_sat_routed: 0.0,
            fee_per_sat_routed: 0.0,
            days_open: 10,
            last_routed: Some(1_700_000_000),
            marginal_profit_30d_sats: 0,
            rebalance_cost_30d_sats: 0,
            opener: "local".to_string(),
            contribution_30d_msat: 0,
            fees_earned_30d_msat: 0,
            sourced_fee_30d_msat: 0,
            forward_count_30d: forward_count,
            sourced_forward_count_30d: sourced_forward_count,
            window_30d_available: true,
        }
    }

    #[test]
    fn disabled_returns_hint_and_no_assembly() {
        let v = build_econ_snapshot(false, None, None, None);
        assert_eq!(v["enabled"], false);
        assert_eq!(v["hint"], "revenue-config set econ_shadow_enabled true");
        assert!(v.get("snapshot").is_none());
    }

    #[test]
    fn channel_read_failure_is_reported_not_fabricated() {
        let v = build_econ_snapshot(
            true,
            Some(SnapshotAssembly::ChannelReadFailed(
                "listpeerchannels: timeout".to_string(),
            )),
            None,
            None,
        );
        assert_eq!(v["enabled"], true);
        assert_eq!(v["snapshot"], Value::Null);
        assert_eq!(
            v["approximations"][0],
            "channel read failed: listpeerchannels: timeout"
        );
    }

    #[test]
    fn assembles_snapshot_from_two_channels_one_with_profitability() {
        let peer_a = format!("02{}", "a".repeat(64));
        let peer_b = format!("03{}", "b".repeat(64));
        let channels = vec![
            raw_channel("111x1x0", &peer_a, 1_000_000, 400_000),
            raw_channel("222x1x0", &peer_b, 2_000_000, 2_000_000),
        ];
        let mut profitability = HashMap::new();
        // >70% outbound over 30d -> OUTBOUND_GATEWAY.
        profitability.insert("111x1x0".to_string(), prof("111x1x0", 20, 1));

        let budget = BudgetPreviewInputs {
            cap_sats: 100_000,
            reserved_sats: 1_000,
            spent_sats: 2_000,
        };
        let (wire, approximations) =
            assemble_snapshot_preview(&channels, &profitability, &budget, 1_700_000_000, 0.5);
        let wire = wire.expect("assembly should succeed for two valid channels");
        assert_eq!(wire["schema_name"], "economic_snapshot");
        let chans = wire["channels"].as_array().unwrap();
        assert_eq!(chans.len(), 2);
        // channels are sorted by channel_id (EconomicSnapshot::new J3).
        assert_eq!(chans[0]["channel_id"], "111x1x0");
        assert_eq!(chans[0]["role"], "OUTBOUND_GATEWAY");
        assert_eq!(chans[0]["lifecycle"], "PRODUCTIVE");
        assert_eq!(chans[0]["confidence_micro"], 0);
        assert_eq!(chans[1]["channel_id"], "222x1x0");
        // No profitability entry for 222x1x0 -> role UNKNOWN, zero economics.
        assert_eq!(chans[1]["role"], "UNKNOWN");
        assert_eq!(chans[1]["exit_revenue_msat"], 0);

        assert_eq!(wire["node"]["daily_budget"]["cap_msat"], 100_000_000);
        assert_eq!(wire["node"]["total_local_msat"], 400_000 + 2_000_000);

        // The 4 Python-documented approximations are always present.
        assert_eq!(approximations.len(), 4);
        assert!(approximations[0].contains("lifecycle=PRODUCTIVE"));
    }

    #[test]
    fn malformed_channel_is_skipped_not_fatal() {
        let good_peer = format!("02{}", "c".repeat(64));
        let channels = vec![
            json!({"short_channel_id": "1x1x1"}), // missing total_msat etc -> build_channel_snapshot errors
            raw_channel("2x1x1", &good_peer, 500_000, 100_000),
        ];
        let profitability = HashMap::new();
        let budget = BudgetPreviewInputs::default();
        let (wire, approximations) =
            assemble_snapshot_preview(&channels, &profitability, &budget, 1_700_000_000, 0.0);
        let wire = wire.expect("one good channel should still produce a snapshot");
        assert_eq!(wire["channels"].as_array().unwrap().len(), 1);
        assert!(approximations
            .iter()
            .any(|a| a.starts_with("channel skipped (")));
    }

    #[test]
    fn missing_short_channel_id_is_skipped_with_explicit_note() {
        let channels = vec![json!({"peer_id": "02aa"})];
        let profitability = HashMap::new();
        let budget = BudgetPreviewInputs::default();
        let (wire, approximations) =
            assemble_snapshot_preview(&channels, &profitability, &budget, 0, 0.0);
        let wire = wire.expect("empty channel set still yields an (empty) snapshot");
        assert_eq!(wire["channels"].as_array().unwrap().len(), 0);
        assert!(approximations
            .iter()
            .any(|a| a.contains("missing short_channel_id")));
    }
}
