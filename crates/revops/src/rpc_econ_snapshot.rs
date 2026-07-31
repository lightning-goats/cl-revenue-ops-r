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
        /// C71-34: notes about evidence the CALLER could not gather,
        /// merged into Python's own `approximations` list. An input
        /// rather than something derived here, because only the caller
        /// knows which source failed.
        evidence_notes: &'a [String],
    },
}

/// C71-34: a profitability read that FAILED, declared.
///
/// Python's `_assemble_econ_snapshot` swallows this into `profitability =
/// {}` with no note (cl-revenue-ops.py:6030-6033), so a snapshot assembled
/// with NO per-channel evidence is indistinguishable from one where every
/// channel legitimately had none. This port keeps Python's response SHAPE
/// -- the degradation is declared through Python's own `approximations`
/// mechanism -- while making the difference visible. Disclosed divergence,
/// in the safe direction: nothing is fabricated either way.
pub const PROFITABILITY_UNAVAILABLE: &str =
    "profitability evidence unavailable: per-channel economics are absent from \
     this snapshot, which is NOT the same as every channel having none";

/// C71-36: one channel whose REQUIRED evidence was refused.
///
/// `gather_profitability` returns `Ok` for the fleet while listing
/// per-channel refusals (a corrupt fee posterior, a missing opener, no
/// `opened_at`) in `FleetProfitability::skipped`. Those channels have no
/// entry in the profitability map, so the snapshot shows them with ZERO
/// economics -- indistinguishable from a channel that genuinely earned and
/// spent nothing. Each one is declared instead.
pub const CHANNEL_EVIDENCE_SKIPPED: &str = "channel evidence skipped";

/// A source this call could not consult at all. Deliberately NOT
/// [`build_econ_snapshot`]'s `enabled=false` shape: an unreadable config
/// surface is not a disabled shadow, and an unreadable budget is not a
/// zero budget.
pub fn build_econ_snapshot_unavailable(code: &str, detail: &str) -> Value {
    json!({"error": code, "detail": detail})
}

/// C71-32: the two intent counters are `null` in shadow mode, and these
/// are the STABLE lines that say why.
///
/// Python counts intents into the PRODUCTION `econ_ledger.db` beside
/// `revenue_ops.db`. This port's `GovernorWiring` writes its own
/// `econ_ledger_dryrun.db` and deliberately never opens Python's file --
/// Python stays authoritative for the whole shadow window. So the two
/// numbers are different populations, and publishing the dry-run count
/// under Python's production field name would be a fabrication that LOOKS
/// real; zero would be worse still, since it asserts "no intents".
///
/// These are shadow-mode answers, NOT production parity. Adopting the
/// production ledger and a same-lifetime session counter is a Task 69
/// cutover blocker.
pub const INTENTS_RECORDED_UNAVAILABLE: &str =
    "intents_recorded_total unavailable: Python's session counter counts intents \
     recorded into the production econ_ledger.db; this port writes only its own \
     econ_ledger_dryrun.db, so no same-population session counter exists yet";
pub const INTENTS_LEDGER_UNAVAILABLE: &str =
    "intents_ledger_total unavailable: the durable count lives in Python's \
     production econ_ledger.db, which this port deliberately never opens while \
     Python remains authoritative";

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
            evidence_notes,
        } => {
            let (wire, approximations) = assemble_snapshot_preview(
                channels,
                profitability,
                budget,
                now,
                receivable_ratio_target,
            );
            // C71-32: a null counter is NEVER emitted undeclared. Pairing
            // the two here rather than at the call site makes it
            // structural -- a caller cannot receive an unexplained null,
            // and supplying real counters later removes the line
            // automatically without touching this builder.
            let mut approximations = approximations;
            approximations.extend(evidence_notes.iter().cloned());
            if intents_recorded_total.is_none() {
                approximations.push(INTENTS_RECORDED_UNAVAILABLE.to_string());
            }
            if intents_ledger_total.is_none() {
                approximations.push(INTENTS_LEDGER_UNAVAILABLE.to_string());
            }
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

/// Task 50 correction round, F1: the wiring layer has NO real
/// `econ_shadow_enabled` config surface to read yet (no `EconShadow`
/// equivalent exists in this Rust port at all). Hardcoding `enabled=false`
/// (what `main.rs` did before this fix) is a FALSE statement about node
/// state on any node where the Python config actually has
/// `econ_shadow_enabled=true` -- indistinguishable from the truthful
/// disabled answer, with no gap marker. This shape is deliberately NOT
/// [`build_econ_snapshot`]'s `enabled=false` shape (2 keys, `enabled`/
/// `hint`) and NOT its `enabled=true` shapes either -- an in-band
/// `not_yet_ported` error so a caller checking `resp["enabled"]` gets
/// neither a (possibly false) `true` nor a (possibly false) `false`.
pub fn build_econ_snapshot_not_wired() -> Value {
    json!({
        "error": "econ shadow not_yet_ported",
        "reason": "the econ_shadow_enabled config surface (the same one \
                   revenue-r-config reads) is not wired into this port yet; \
                   this is NOT a truthful disabled/enabled answer",
    })
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
    fn not_wired_shape_is_neither_python_disabled_nor_enabled_shape() {
        let v = build_econ_snapshot_not_wired();
        // Must NOT be readable as Python's truthful `enabled: false`.
        assert_ne!(v.get("enabled"), Some(&json!(false)));
        assert_ne!(v.get("enabled"), Some(&json!(true)));
        assert_eq!(v["error"], "econ shadow not_yet_ported");
        assert!(
            v.get("hint").is_none(),
            "must not reuse the disabled shape's key"
        );
        assert!(
            v.get("snapshot").is_none(),
            "must not reuse the enabled shape's key"
        );
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

    // -----------------------------------------------------------------
    // C71-32: the intent counters in shadow mode.
    //
    // Python counts intents into the PRODUCTION `econ_ledger.db`; this
    // port writes only `econ_ledger_dryrun.db` and never opens Python's
    // file while Python stays authoritative. The two numbers are
    // different populations, so the dry-run count must not be published
    // under Python's field name -- and zero would be worse, because it
    // asserts "no intents recorded" rather than "we cannot know".
    // -----------------------------------------------------------------

    fn ready_snapshot(recorded: Option<i64>, ledger: Option<i64>) -> Value {
        let channels = vec![raw_channel(
            "111x1x0",
            &format!("02{}", "a".repeat(64)),
            1_000_000,
            400_000,
        )];
        let profitability = HashMap::new();
        let budget = BudgetPreviewInputs::default();
        build_econ_snapshot(
            true,
            Some(SnapshotAssembly::Ready {
                channels: &channels,
                profitability: &profitability,
                budget: &budget,
                now: 1_800_000_000,
                receivable_ratio_target: 0.0,
                evidence_notes: &[],
            }),
            recorded,
            ledger,
        )
    }

    #[test]
    fn shadow_mode_reports_null_intent_counters_and_says_why() {
        let v = ready_snapshot(None, None);
        assert_eq!(v["intents_recorded_total"], Value::Null);
        assert_eq!(v["intents_ledger_total"], Value::Null);

        let approximations: Vec<&str> = v["approximations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a.as_str().unwrap())
            .collect();
        assert!(
            approximations.contains(&INTENTS_RECORDED_UNAVAILABLE),
            "a null counter must never be emitted undeclared: {approximations:?}"
        );
        assert!(
            approximations.contains(&INTENTS_LEDGER_UNAVAILABLE),
            "{approximations:?}"
        );
    }

    #[test]
    fn a_null_counter_is_never_reported_as_zero() {
        // Zero is a Python-legal value meaning "no intents recorded this
        // session". Substituting it for "unknown" is the fabrication this
        // whole treatment exists to avoid.
        let v = ready_snapshot(None, None);
        assert_ne!(v["intents_recorded_total"], json!(0));
        assert_ne!(v["intents_ledger_total"], json!(0));
    }

    /// The path Task 69 will use once the production ledger is adopted:
    /// real counters flow through UNCHANGED, and the shadow-mode
    /// approximation lines disappear on their own. If this ever fails,
    /// supplying production counters would require editing the response
    /// builder, which is exactly the coupling this test prevents.
    #[test]
    fn real_counters_pass_through_as_integers_without_the_shadow_notes() {
        let v = ready_snapshot(Some(3), Some(7));
        assert_eq!(v["intents_recorded_total"], json!(3));
        assert_eq!(v["intents_ledger_total"], json!(7));

        let approximations: Vec<&str> = v["approximations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a.as_str().unwrap())
            .collect();
        assert!(!approximations.contains(&INTENTS_RECORDED_UNAVAILABLE));
        assert!(!approximations.contains(&INTENTS_LEDGER_UNAVAILABLE));
    }

    /// A real zero is NOT the unknown case: it passes through as 0 with no
    /// note, because "the session recorded none" is a fact.
    #[test]
    fn a_genuine_zero_counter_is_reported_as_zero() {
        let v = ready_snapshot(Some(0), Some(0));
        assert_eq!(v["intents_recorded_total"], json!(0));
        assert_eq!(v["intents_ledger_total"], json!(0));
        let approximations: Vec<&str> = v["approximations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a.as_str().unwrap())
            .collect();
        assert!(!approximations.contains(&INTENTS_RECORDED_UNAVAILABLE));
    }

    /// Each counter is declared independently -- a half-available pair
    /// must not hide the missing half behind the present one.
    #[test]
    fn only_the_missing_counter_is_declared() {
        let v = ready_snapshot(Some(5), None);
        assert_eq!(v["intents_recorded_total"], json!(5));
        assert_eq!(v["intents_ledger_total"], Value::Null);
        let approximations: Vec<&str> = v["approximations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a.as_str().unwrap())
            .collect();
        assert!(!approximations.contains(&INTENTS_RECORDED_UNAVAILABLE));
        assert!(approximations.contains(&INTENTS_LEDGER_UNAVAILABLE));
    }

    /// C71-34: a caller that could not gather profitability declares it,
    /// and the note reaches Python's own `approximations` list.
    ///
    /// Python swallows this failure into `profitability = {}` with no
    /// note, so a snapshot with NO evidence looks exactly like one where
    /// every channel legitimately had none.
    #[test]
    fn a_declared_evidence_failure_reaches_the_approximations_list() {
        let channels = vec![raw_channel(
            "111x1x0",
            &format!("02{}", "a".repeat(64)),
            1_000_000,
            400_000,
        )];
        let profitability = HashMap::new();
        let budget = BudgetPreviewInputs::default();
        let notes = vec![format!("{PROFITABILITY_UNAVAILABLE}: store unavailable")];
        let v = build_econ_snapshot(
            true,
            Some(SnapshotAssembly::Ready {
                channels: &channels,
                profitability: &profitability,
                budget: &budget,
                now: 1_800_000_000,
                receivable_ratio_target: 0.0,
                evidence_notes: &notes,
            }),
            None,
            None,
        );
        let approximations: Vec<&str> = v["approximations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a.as_str().unwrap())
            .collect();
        assert!(
            approximations
                .iter()
                .any(|a| a.starts_with(PROFITABILITY_UNAVAILABLE)),
            "an absent-evidence snapshot must say so: {approximations:?}"
        );
        // And the snapshot is still produced -- Python degrades here, it
        // does not refuse, and the shape must stay Python's.
        assert_eq!(v["enabled"], true);
        assert!(v["snapshot"].is_object());
    }

    /// The control: no declared failure means no note. Without this,
    /// pushing the line unconditionally would pass the test above while
    /// telling every healthy caller its evidence was missing.
    #[test]
    fn a_healthy_pass_carries_no_evidence_failure_note() {
        let v = ready_snapshot(None, None);
        let approximations: Vec<&str> = v["approximations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a.as_str().unwrap())
            .collect();
        assert!(!approximations
            .iter()
            .any(|a| a.starts_with(PROFITABILITY_UNAVAILABLE)));
    }
}
