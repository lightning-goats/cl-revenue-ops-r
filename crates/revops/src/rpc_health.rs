//! Pure response builder for `revenue-r-health`.
//!
//! Port of `revenue_health` (cl-revenue-ops.py:6181-6357). Section 1
//! (`financials.today`/`.week`) is DB-backed via
//! `profitability_analyzer.get_pnl_summary` (ported as
//! `revops_db::queries::pnl_summary`, already used by `rpc_dashboard`) and
//! is fully wired here. Section 1's `annualized_roc_pct`
//! (`calculate_roc`, profitability_analyzer.py:1817-1861) needs total
//! fleet channel capacity from a live `listpeerchannels` RPC call this
//! DB-only builder cannot perform itself -- the caller MAY supply an
//! already-fetched `total_capacity_sats`, else it is `null` and
//! gap-listed, exactly like `rpc_dashboard`'s `tlv_sats`.
//!
//! Task 66 slice 8d closed most of the former gaps via
//! [`apply_health_extras`]: `channels` (classification counts over the
//! same profitability gather `revenue-profitability` runs), `fees` (the
//! live controller's counts through `FeeDebugQuery::HealthCounts` /
//! `revops_fees::cycle::fee_health_counts`), `budget` (the
//! total-cost-budget status subset, py 6279-6291), and `top_routes`
//! (`queries::top_route_pairs`, py 6326-6339). Each follows Python's
//! per-section try/except: a section whose fetch FAILED renders
//! `{"error": ...}` (a real py arm, not a gap); a section whose pipeline
//! is genuinely unwired in this runtime (no observer store, no running
//! scheduler) stays `null` + gap-listed. `rebalancer` and `planner`
//! remain gap-listed deliberately: their owners exist but the engine /
//! capacity-planner capabilities are unassembled until Task 69's
//! authority-gated construction, and their Python sections read live
//! engine internals this port cannot honestly synthesize yet. `boltz` is
//! answered honestly (`{"enabled": false}`, Python's own no-manager
//! answer); `loops` is the Rust-owned durable inventory
//! ([`build_health_with_loops`]).

use revops_db::queries::PnlSummary;
use revops_econ::pyfloat::py_round;
use serde_json::{json, Value};

/// Port of `ChannelProfitabilityAnalyzer.calculate_roc`'s annualization
/// step (profitability_analyzer.py:1846-1855), given an ALREADY-FETCHED
/// total fleet capacity (the live `listpeerchannels` sum this DB-only
/// builder cannot compute itself). Mirrors the exact zero-capacity
/// fallback (`0.0`, never a division).
fn annualized_roc_pct(net_profit_sats: i64, total_capacity_sats: i64, window_days: i64) -> f64 {
    if total_capacity_sats <= 0 {
        return 0.0;
    }
    let roc_pct = (net_profit_sats as f64 / total_capacity_sats as f64) * 100.0;
    let annualized = roc_pct * (365.0 / window_days.max(1) as f64);
    py_round(annualized, 2)
}

/// Port of `revenue_health`. `pnl_1d`/`pnl_7d` are
/// `profitability_analyzer.get_pnl_summary(1)`/`(7)`'s already-fetched
/// results; `total_capacity_sats` is the optional already-fetched live
/// capacity sum for `annualized_roc_pct` (see module doc comment).
pub fn build_health(
    generated_at: i64,
    pnl_1d: Option<&PnlSummary>,
    pnl_7d: Option<&PnlSummary>,
    total_capacity_sats: Option<i64>,
) -> Value {
    let mut gaps: Vec<&'static str> = Vec::new();

    let financials = match (pnl_1d, pnl_7d) {
        (Some(d1), Some(d7)) => {
            let roc = match total_capacity_sats {
                Some(cap) => json!(annualized_roc_pct(d7.net_profit_sats, cap, d7.window_days)),
                None => {
                    gaps.push("financials.week.annualized_roc_pct");
                    Value::Null
                }
            };
            json!({
                "today": {
                    "revenue_sats": d1.gross_revenue_sats,
                    "costs_sats": d1.opex_sats,
                    "net_profit_sats": d1.net_profit_sats,
                    "forward_count": d1.forward_count,
                    "volume_sats": d1.volume_sats,
                },
                "week": {
                    "revenue_sats": d7.gross_revenue_sats,
                    "costs_sats": d7.opex_sats,
                    "net_profit_sats": d7.net_profit_sats,
                    "forward_count": d7.forward_count,
                    "operating_margin_pct": py_round(d7.operating_margin_pct, 1),
                    "annualized_roc_pct": roc,
                },
            })
        }
        _ => {
            gaps.push("financials");
            Value::Null
        }
    };

    for g in [
        "channels",
        "fees",
        "rebalancer",
        "budget",
        "planner",
        "top_routes",
        "loops",
    ] {
        gaps.push(g);
    }

    json!({
        "generated_at": generated_at,
        "financials": financials,
        "channels": Value::Null,
        "fees": Value::Null,
        "rebalancer": Value::Null,
        "budget": Value::Null,
        // py 6301-6315: `{"enabled": false}` is py's answer when no
        // Boltz manager is wired OR its `enabled` flag is false.
        //
        // SELF-REVIEW 2026-07-31: the Task-50 justification ("no Boltz
        // manager is wired here") went STALE when Task 63 landed a Boltz
        // runtime in this same binary -- with Boltz enabled, this section
        // would claim disabled while `revenue-boltz-auto-cycle-status`
        // says enabled. The caller now supplies the real answer through
        // `HealthExtras::boltz`; this literal is only the no-runtime
        // default, which IS py's arm for that condition.
        "boltz": json!({"enabled": false}),
        "planner": Value::Null,
        "top_routes": Value::Null,
        "loops": Value::Null,
        "_gaps": gaps,
    })
}

/// Task 67: `boot_id` is THIS process's identity. Every loop's
/// `current_status` is judged against it, so a pass that completed in a
/// prior boot reports `never_run_this_boot` rather than being inherited --
/// the defect the Task 11 audit named. The raw terminal fields are still
/// emitted (including `terminal_boot_id`), so the verdict changes without
/// erasing the history an operator needs.
pub fn build_health_with_loops(
    generated_at: i64,
    pnl_1d: Option<&PnlSummary>,
    pnl_7d: Option<&PnlSummary>,
    total_capacity_sats: Option<i64>,
    loops: Result<&[revops_db::loop_health::LoopHealthRow], String>,
    boot_id: &str,
) -> Value {
    let mut value = build_health(generated_at, pnl_1d, pnl_7d, total_capacity_sats);
    value["loops"] = match loops {
        Ok(rows) => Value::Array(
            rows.iter()
                .map(|row| {
                    json!({
                        "loop_name": row.loop_id.as_str(),
                        "wiring_status": row.wiring_status.as_str(),
                        "generation": row.generation,
                        "terminal_generation": row.terminal_generation,
                        "terminal_status": row.terminal_status.as_str(),
                        "runtime_status": row.runtime_status.as_str(),
                        "last_suspended_at": row.last_suspended_at,
                        "last_suspension_reason": row.last_suspension_reason,
                        "last_started_at": row.last_started_at,
                        "last_passed_at": row.last_passed_at,
                        "last_error_at": row.last_error_at,
                        "last_error": row.last_error,
                        "coalesced_total": row.coalesced_total,
                        "dropped_total": row.dropped_total,
                        "updated_at": row.updated_at,
                        "boot_id": row.boot_id,
                        "terminal_boot_id": row.terminal_boot_id,
                        "current_status": revops_db::loop_health::current_boot_status(
                            row, boot_id,
                        )
                        .as_str(),
                    })
                })
                .collect(),
        ),
        Err(error) => json!({"error": error}),
    };
    // Name the boot the verdicts are judged against, so `current_status`
    // is attributable instead of an unexplained string.
    value["boot_id"] = json!(boot_id);
    if let Some(gaps) = value["_gaps"].as_array_mut() {
        gaps.retain(|gap| gap != "loops");
    }
    value
}

/// The census-closed sections' prefetched inputs (Task 66 slice 8d).
/// The three-state encoding per section: `None` = the pipeline is
/// UNWIRED in this runtime (stays null + gap-listed); `Some(Err(e))` =
/// the fetch ran and FAILED (Python's per-section except arm,
/// `{"error": e}` -- a real answer, gap pruned); `Some(Ok(..))` = real
/// data. `top_routes` has no error form: Python's except arm there is
/// `[]` (cl-revenue-ops.py:6338-6339), so the caller maps a fetch
/// failure to `Some(vec![])`.
/// The channels section's fetched form: `(total, classification -> count)`.
pub type ChannelClassCounts = (usize, std::collections::BTreeMap<String, i64>);

pub struct HealthExtras {
    /// py 6216-6229.
    pub channels: Option<Result<ChannelClassCounts, String>>,
    /// The owner's `fee_health_counts` payload, py 6232-6259 (the
    /// bounded-bridge error Value doubles as py's except arm).
    pub fees: Option<Value>,
    /// The FULL total-cost-budget status response, py 6279-6293; the
    /// subset projection happens here.
    pub budget: Option<Result<Value, String>>,
    pub top_routes: Option<Vec<revops_db::queries::TopRoutePair>>,
    /// py 6301-6315: `Some(value)` replaces the no-runtime default with
    /// the live auto-cycle snapshot (or py's `{"enabled": false}` when
    /// the manager exists but its flag is off). `None` leaves the
    /// default -- correct only when no Boltz runtime exists at all.
    pub boltz: Option<Value>,
}

/// Fill the census-closed sections into a [`build_health_with_loops`]
/// value and prune exactly the gap entries that now carry real answers.
pub fn apply_health_extras(value: &mut Value, extras: HealthExtras) {
    let mut filled: Vec<&'static str> = Vec::new();

    if let Some(channels) = extras.channels {
        value["channels"] = match channels {
            Ok((total, classifications)) => json!({
                "total": total,
                "classifications": classifications,
            }),
            Err(error) => json!({"error": error}),
        };
        filled.push("channels");
    }

    if let Some(fees) = extras.fees {
        value["fees"] = fees;
        filled.push("fees");
    }

    if let Some(budget) = extras.budget {
        value["budget"] = match budget {
            Ok(status) => {
                // py 6281-6291: the subset projection. A status carrying
                // an "error" key is the raised-provider case -> py's
                // except arm, never zeros dressed as a real budget.
                match status.get("error") {
                    Some(error) => json!({"error": error}),
                    None => {
                        let actual_spent = status
                            .get("actual_spent_sats")
                            .and_then(Value::as_i64)
                            .unwrap_or(0);
                        let effective = status
                            .get("effective_budget_sats")
                            .cloned()
                            .unwrap_or(json!(0));
                        let utilization = py_round(
                            100.0 * actual_spent as f64
                                / (effective.as_i64().unwrap_or(0).max(1)) as f64,
                            1,
                        );
                        json!({
                            "effective_budget_sats": effective,
                            "total_spent_sats": actual_spent,
                            "remaining_sats": status.get("remaining_sats").cloned().unwrap_or(json!(0)),
                            "spent_by_category": status
                                .get("actual_spent_by_category")
                                .cloned()
                                .unwrap_or(json!({})),
                            "utilization_pct": utilization,
                        })
                    }
                }
            }
            Err(error) => json!({"error": error}),
        };
        filled.push("budget");
    }

    if let Some(boltz) = extras.boltz {
        value["boltz"] = boltz;
    }

    if let Some(routes) = extras.top_routes {
        // py 6329-6337: normalized scids, floor msat->sats, count.
        value["top_routes"] = Value::Array(
            routes
                .iter()
                .map(|route| {
                    json!({
                        "in_channel": route.in_channel.replace(':', "x"),
                        "out_channel": route.out_channel.replace(':', "x"),
                        "fee_sats_7d": route.total_fee_msat.div_euclid(1000),
                        "forward_count": route.forward_count,
                    })
                })
                .collect(),
        );
        filled.push("top_routes");
    }

    if let Some(gaps) = value["_gaps"].as_array_mut() {
        gaps.retain(|gap| {
            gap.as_str()
                .map(|gap| !filled.contains(&gap))
                .unwrap_or(true)
        });
    }
}

#[cfg(test)]
mod extras_tests {
    use super::*;
    use serde_json::json;

    fn base() -> Value {
        build_health(1_800_000_000, None, None, None)
    }

    fn remaining_gaps(v: &Value) -> Vec<String> {
        v["_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g.as_str().unwrap().to_string())
            .collect()
    }

    /// Every wired section fills and prunes its gap; rebalancer/planner
    /// stay gap-listed (Task-69 staging).
    #[test]
    fn filled_sections_prune_their_gaps_only() {
        let mut v = base();
        let mut classifications = std::collections::BTreeMap::new();
        classifications.insert("profitable".to_string(), 3i64);
        classifications.insert("zombie".to_string(), 1i64);
        apply_health_extras(
            &mut v,
            HealthExtras {
                channels: Some(Ok((4, classifications))),
                fees: Some(json!({
                    "managed_channels": 4, "converged": 2,
                    "still_learning": 2, "sleeping": 1,
                })),
                budget: Some(Ok(json!({
                    "effective_budget_sats": 5000,
                    "actual_spent_sats": 811,
                    "remaining_sats": 3980,
                    "actual_spent_by_category": {"rebalance": 811},
                }))),
                boltz: None,
                top_routes: Some(vec![revops_db::queries::TopRoutePair {
                    in_channel: "1:1:0".to_string(),
                    out_channel: "2x2x0".to_string(),
                    total_fee_msat: 6999,
                    forward_count: 2,
                }]),
            },
        );
        assert_eq!(v["channels"]["total"], 4);
        assert_eq!(v["channels"]["classifications"]["profitable"], 3);
        assert_eq!(v["fees"]["converged"], 2);
        // py 6281-6291 subset projection + round-1 utilization:
        // 100*810/5000 = 16.2.
        assert_eq!(v["budget"]["total_spent_sats"], 811);
        assert_eq!(v["budget"]["spent_by_category"]["rebalance"], 811);
        assert_eq!(
            v["budget"]["utilization_pct"], 16.2,
            "py round(16.22, 1) -- the third decimal makes round-1 observable"
        );
        // py 6329-6337: normalize + floor msat -> sats.
        assert_eq!(v["top_routes"][0]["in_channel"], "1x1x0");
        assert_eq!(v["top_routes"][0]["fee_sats_7d"], 6, "floor(6999 msat)");
        let gaps = remaining_gaps(&v);
        assert!(gaps.contains(&"rebalancer".to_string()), "{gaps:?}");
        assert!(gaps.contains(&"planner".to_string()));
        assert!(!gaps.contains(&"channels".to_string()));
        assert!(!gaps.contains(&"fees".to_string()));
        assert!(!gaps.contains(&"budget".to_string()));
        assert!(!gaps.contains(&"top_routes".to_string()));
    }

    /// py's per-section except arms: a FAILED fetch is {"error": e} and
    /// NOT a gap; an unwired pipeline (None) keeps null + the gap entry.
    #[test]
    fn failed_fetch_is_pys_error_arm_unwired_stays_a_gap() {
        let mut v = base();
        apply_health_extras(
            &mut v,
            HealthExtras {
                channels: Some(Err("listpeerchannels failed".to_string())),
                fees: None,
                budget: Some(Err("budget provider raised".to_string())),
                boltz: Some(json!({"enabled": true})),
                top_routes: Some(vec![]),
            },
        );
        assert_eq!(v["channels"]["error"], "listpeerchannels failed");
        assert_eq!(v["fees"], Value::Null);
        assert_eq!(v["budget"]["error"], "budget provider raised");
        assert_eq!(v["top_routes"], json!([]), "py except -> []");
        assert_eq!(
            v["boltz"],
            json!({"enabled": true}),
            "a live Boltz runtime's real state replaces the no-runtime default"
        );
        let gaps = remaining_gaps(&v);
        assert!(gaps.contains(&"fees".to_string()), "unwired keeps its gap");
        assert!(
            !gaps.contains(&"channels".to_string()),
            "an error IS an answer"
        );
        assert!(!gaps.contains(&"budget".to_string()));
        assert!(!gaps.contains(&"top_routes".to_string()));
    }

    /// A budget status that itself carries an "error" key (the raised
    /// provider case) must surface as py's except arm, never as a
    /// zero-valued budget dressed as real.
    #[test]
    fn error_shaped_budget_status_never_projects_zeros() {
        let mut v = base();
        apply_health_extras(
            &mut v,
            HealthExtras {
                channels: None,
                fees: None,
                budget: Some(Ok(json!({"error": "Plugin not initialized"}))),
                boltz: None,
                top_routes: None,
            },
        );
        assert_eq!(v["budget"], json!({"error": "Plugin not initialized"}));
        assert!(v["budget"].get("effective_budget_sats").is_none());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pnl(
        window_days: i64,
        gross: i64,
        opex: i64,
        volume: i64,
        forwards: i64,
        margin: f64,
    ) -> PnlSummary {
        PnlSummary {
            window_days,
            gross_revenue_sats: gross,
            opex_sats: opex,
            rebalance_cost_sats: opex,
            closure_cost_sats: 0,
            net_profit_sats: gross - opex,
            operating_margin_pct: margin,
            volume_sats: volume,
            forward_count: forwards,
        }
    }

    #[test]
    fn missing_pnl_yields_null_financials_and_gap() {
        let v = build_health(1000, None, None, None);
        assert_eq!(v["financials"], Value::Null);
        let gaps: Vec<&str> = v["_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g.as_str().unwrap())
            .collect();
        assert!(gaps.contains(&"financials"));
    }

    #[test]
    fn wired_pnl_populates_today_and_week() {
        let d1 = pnl(1, 1000, 200, 50_000, 12, 80.0);
        let d7 = pnl(7, 5000, 900, 300_000, 60, 75.451);
        let v = build_health(1000, Some(&d1), Some(&d7), None);
        assert_eq!(v["financials"]["today"]["revenue_sats"], 1000);
        assert_eq!(v["financials"]["today"]["net_profit_sats"], 800);
        assert_eq!(v["financials"]["week"]["revenue_sats"], 5000);
        // round(75.451, 1) == 75.5 (python3-verified golden) -- control on
        // py_round actually being applied to 1 decimal, not just passed
        // through unrounded.
        assert_eq!(v["financials"]["week"]["operating_margin_pct"], 75.5);
        assert_eq!(v["financials"]["week"]["annualized_roc_pct"], Value::Null);
        let gaps: Vec<&str> = v["_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g.as_str().unwrap())
            .collect();
        assert!(gaps.contains(&"financials.week.annualized_roc_pct"));
        assert!(!gaps.contains(&"financials"));
    }

    #[test]
    fn annualized_roc_computed_when_capacity_supplied() {
        let d1 = pnl(1, 100, 0, 1000, 1, 100.0);
        let d7 = pnl(7, 700, 0, 7000, 7, 100.0);
        // net_profit_7d=700, capacity=10_000 -> roc_pct=7.0 -> annualized =
        // 7.0 * (365/7) = 365.0
        let v = build_health(0, Some(&d1), Some(&d7), Some(10_000));
        assert_eq!(v["financials"]["week"]["annualized_roc_pct"], 365.0);
        let gaps: Vec<&str> = v["_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g.as_str().unwrap())
            .collect();
        assert!(!gaps.contains(&"financials.week.annualized_roc_pct"));
    }

    #[test]
    fn zero_capacity_falls_back_to_zero_not_division() {
        let d1 = pnl(1, 100, 0, 1000, 1, 100.0);
        let d7 = pnl(7, 700, 0, 7000, 7, 100.0);
        let v = build_health(0, Some(&d1), Some(&d7), Some(0));
        assert_eq!(v["financials"]["week"]["annualized_roc_pct"], 0.0);
    }

    #[test]
    fn always_present_static_gaps_and_never_fabricated_values() {
        let v = build_health(0, None, None, None);
        for field in [
            "channels",
            "fees",
            "rebalancer",
            "budget",
            "planner",
            "top_routes",
            "loops",
        ] {
            assert_eq!(
                v[field],
                Value::Null,
                "{field} must be null, not fabricated"
            );
        }
        let gaps: Vec<&str> = v["_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g.as_str().unwrap())
            .collect();
        for field in [
            "channels",
            "fees",
            "rebalancer",
            "budget",
            "planner",
            "top_routes",
            "loops",
        ] {
            assert!(gaps.contains(&field), "{field} must be gap-listed");
        }
    }

    /// Task 50 correction round ("should NOT stay gaps" item): with no
    /// Boltz manager wired in this port, Python's OWN answer for `boltz`
    /// is the definite `{"enabled": false}` (cl-revenue-ops.py:6312-6313)
    /// -- cheap, true, and shape-faithful. `null` + a `_gaps` entry is
    /// strictly worse (it hides a field that IS computable today).
    #[test]
    fn boltz_section_is_the_honest_enabled_false_shape_not_a_null_gap() {
        let v = build_health(0, None, None, None);
        assert_eq!(v["boltz"], json!({"enabled": false}));
        let gaps: Vec<&str> = v["_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g.as_str().unwrap())
            .collect();
        assert!(
            !gaps.contains(&"boltz"),
            "boltz is no longer a gap once populated: {gaps:?}"
        );
    }
    #[test]
    fn durable_loop_rows_replace_only_the_loops_gap() {
        let rows: Vec<revops_db::loop_health::LoopHealthRow> =
            revops_db::loop_health::REQUIRED_LOOPS
                .into_iter()
                .enumerate()
                .map(|(index, id)| {
                    let mut row = revops_db::loop_health::LoopHealthRow::new(
                        id,
                        if id == revops_db::loop_health::LoopId::Fee {
                            revops_db::loop_health::WiringStatus::Ready
                        } else {
                            revops_db::loop_health::WiringStatus::NotWired
                        },
                        100 + index as i64,
                    );
                    row.generation = index as u64;
                    row.coalesced_total = 10 + index as u64;
                    row.dropped_total = 20 + index as u64;
                    row
                })
                .collect();
        let value = build_health_with_loops(200, None, None, None, Ok(&rows), "boot-test");
        // Task 67: eight loops, Python's label vocabulary, in
        // REQUIRED_LOOPS order (flow-analysis first).
        assert_eq!(value["loops"].as_array().unwrap().len(), 8);
        assert_eq!(value["loops"][0]["loop_name"], "flow-analysis");
        assert_eq!(value["loops"][1]["loop_name"], "fee-adjustment");
        assert_eq!(value["loops"][0]["wiring_status"], "not_wired");
        assert_eq!(value["loops"][7]["dropped_total"], 27);
        assert!(!value["_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|gap| gap == "loops"));
    }

    #[test]
    fn same_second_new_generation_is_incomplete_not_prior_passed() {
        let mut row = revops_db::loop_health::LoopHealthRow::new(
            revops_db::loop_health::LoopId::Fee,
            revops_db::loop_health::WiringStatus::Ready,
            100,
        );
        row.generation = 2;
        row.terminal_generation = 1;
        row.last_started_at = Some(100);
        row.last_passed_at = Some(100);
        // Task 67: an in-flight generation counts only for the boot that
        // STARTED it, so this row must name the boot under judgement.
        row.boot_id = Some("boot-test".to_string());
        let value = build_health_with_loops(100, None, None, None, Ok(&[row]), "boot-test");
        assert_eq!(value["loops"][0]["current_status"], "incomplete");
    }

    #[test]
    fn same_second_terminal_kind_is_not_inferred_from_timestamps() {
        use revops_db::loop_health::{LoopHealthRow, LoopId, TerminalStatus, WiringStatus};
        let mut errored = LoopHealthRow::new(LoopId::Fee, WiringStatus::Ready, 100);
        errored.generation = 2;
        errored.terminal_generation = 2;
        errored.last_passed_at = Some(100);
        errored.last_error_at = Some(100);
        errored.terminal_status = TerminalStatus::Error;
        errored.boot_id = Some("boot-test".to_string());
        errored.terminal_boot_id = Some("boot-test".to_string());
        let error_value =
            build_health_with_loops(100, None, None, None, Ok(&[errored]), "boot-test");
        assert_eq!(error_value["loops"][0]["current_status"], "error");
        let mut passed = LoopHealthRow::new(LoopId::Fee, WiringStatus::Ready, 100);
        passed.generation = 2;
        passed.terminal_generation = 2;
        passed.last_passed_at = Some(100);
        passed.last_error_at = Some(100);
        passed.terminal_status = TerminalStatus::Passed;
        passed.boot_id = Some("boot-test".to_string());
        passed.terminal_boot_id = Some("boot-test".to_string());
        let pass_value = build_health_with_loops(100, None, None, None, Ok(&[passed]), "boot-test");
        assert_eq!(pass_value["loops"][0]["current_status"], "passed");
    }

    #[test]
    fn durable_suspension_takes_precedence_over_a_later_terminal_pass() {
        use revops_db::loop_health::{
            LoopHealthRow, LoopId, RuntimeStatus, TerminalStatus, WiringStatus,
        };
        let mut row = LoopHealthRow::new(LoopId::Fee, WiringStatus::Ready, 100);
        row.generation = 1;
        row.terminal_generation = 1;
        row.terminal_status = TerminalStatus::Passed;
        row.last_passed_at = Some(102);
        row.runtime_status = RuntimeStatus::Suspended;
        row.last_suspended_at = Some(101);
        row.last_suspension_reason = Some("backpressure persistence failed".to_string());
        let value = build_health_with_loops(103, None, None, None, Ok(&[row]), "boot-test");
        assert_eq!(value["loops"][0]["terminal_status"], "passed");
        assert_eq!(value["loops"][0]["runtime_status"], "suspended");
        assert_eq!(value["loops"][0]["current_status"], "suspended");
        assert_eq!(value["loops"][0]["last_suspended_at"], 101);
        assert_eq!(
            value["loops"][0]["last_suspension_reason"],
            "backpressure persistence failed"
        );
    }

    #[test]
    fn loop_store_failure_is_section_local_and_not_fabricated() {
        let value = build_health_with_loops(
            200,
            None,
            None,
            None,
            Err("observer actor gone".to_string()),
            "boot-test",
        );
        assert_eq!(value["loops"]["error"], "observer actor gone");
        assert!(value.get("generated_at").is_some());
        assert!(value["financials"].is_null());
        assert!(!value["_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|gap| gap == "loops"));
    }
}
