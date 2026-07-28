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
//! Sections 2-9 (channel classifications, fee-controller convergence,
//! rebalancer state, unified budget, Boltz auto-cycle, capacity planner,
//! top routing-fee route pairs, daemon-loop heartbeat liveness) are
//! unconditionally `null` and gap-listed here -- not a fabricated
//! zero/empty value. Round-2 correction, P2 (codex re-review): the "fees"
//! gap specifically is NOT "no Rust fee controller exists" -- it does:
//! `revops-fees::cycle`'s `ControllerState` (`cycle.rs:534`) is a real,
//! running fee-decision kernel driven by `fee_scheduler.rs`'s scheduler
//! loop (see the fee control stack in `docs/port/PARITY-CHECKLIST.md`
//! §1, all sixteen components `[x]`). The gap is narrower: this
//! `revenue-r-health` handler is not GIVEN that live `ControllerState` --
//! nothing plumbs the scheduler's current state into `build_health`'s
//! caller in `main.rs` yet, so `fees` stays `null`+gap-listed for lack of
//! wiring, not for lack of a controller to read from. Rebalancer state,
//! capacity planner, and top routing-fee route pairs genuinely have no
//! Rust-side equivalent running in this plugin at all (no rebalance loop,
//! no capacity-planner daemon, no route-tracking); Boltz auto-cycle
//! likewise has no Rust manager wired (see `boltz` below, which IS
//! answered honestly rather than gapped); and the `_loop_heartbeats`
//! registry (cl-revenue-ops.py:322-323) is a Python-only in-memory
//! structure this plugin has no counterpart for.

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
        // Task 50 correction round ("should NOT stay gaps"): no Boltz
        // manager is wired here, but that IS Python's own truthful answer
        // for this exact condition (cl-revenue-ops.py:6312-6313) -- not a
        // gap, a real (if minimal) computed value.
        "boltz": json!({"enabled": false}),
        "planner": Value::Null,
        "top_routes": Value::Null,
        "loops": Value::Null,
        "_gaps": gaps,
    })
}

pub fn build_health_with_loops(
    generated_at: i64,
    pnl_1d: Option<&PnlSummary>,
    pnl_7d: Option<&PnlSummary>,
    total_capacity_sats: Option<i64>,
    loops: Result<&[revops_db::loop_health::LoopHealthRow], String>,
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
                        "last_started_at": row.last_started_at,
                        "last_passed_at": row.last_passed_at,
                        "last_error_at": row.last_error_at,
                        "last_error": row.last_error,
                        "coalesced_total": row.coalesced_total,
                        "dropped_total": row.dropped_total,
                        "updated_at": row.updated_at,
                        "current_status": current_loop_status(row),
                    })
                })
                .collect(),
        ),
        Err(error) => json!({"error": error}),
    };
    if let Some(gaps) = value["_gaps"].as_array_mut() {
        gaps.retain(|gap| gap != "loops");
    }
    value
}

fn current_loop_status(row: &revops_db::loop_health::LoopHealthRow) -> &'static str {
    use revops_db::loop_health::WiringStatus;
    if row.wiring_status == WiringStatus::NotWired {
        return "not_wired";
    }
    if row.generation > row.terminal_generation {
        return "incomplete";
    }
    match row.terminal_status {
        revops_db::loop_health::TerminalStatus::Passed => "passed",
        revops_db::loop_health::TerminalStatus::Error => "error",
        revops_db::loop_health::TerminalStatus::None => "never_run",
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
        let value = build_health_with_loops(200, None, None, None, Ok(&rows));
        assert_eq!(value["loops"].as_array().unwrap().len(), 5);
        assert_eq!(value["loops"][0]["loop_name"], "fee");
        assert_eq!(value["loops"][1]["wiring_status"], "not_wired");
        assert_eq!(value["loops"][4]["dropped_total"], 24);
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
        let value = build_health_with_loops(100, None, None, None, Ok(&[row]));
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
        let error_value = build_health_with_loops(100, None, None, None, Ok(&[errored]));
        assert_eq!(error_value["loops"][0]["current_status"], "error");
        let mut passed = LoopHealthRow::new(LoopId::Fee, WiringStatus::Ready, 100);
        passed.generation = 2;
        passed.terminal_generation = 2;
        passed.last_passed_at = Some(100);
        passed.last_error_at = Some(100);
        passed.terminal_status = TerminalStatus::Passed;
        let pass_value = build_health_with_loops(100, None, None, None, Ok(&[passed]));
        assert_eq!(pass_value["loops"][0]["current_status"], "passed");
    }

    #[test]
    fn loop_store_failure_is_section_local_and_not_fabricated() {
        let value = build_health_with_loops(
            200,
            None,
            None,
            None,
            Err("observer actor gone".to_string()),
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
