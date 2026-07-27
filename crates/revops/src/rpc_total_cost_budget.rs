//! Pure response builder for `revenue-total-cost-budget`.
//!
//! Port of `_compute_total_cost_budget_status` (cl-revenue-ops.py:8350-
//! 8482), reached via `revenue_total_cost_budget` -> `_total_cost_budget_status`
//! (cl-revenue-ops.py:7705-7711, 8296-8345 -- the memo/re-entrancy wrapper is
//! process-local caching state, not part of the response shape, so it is
//! not reproduced here).
//!
//! # What is genuinely ported vs. declared as a gap
//!
//! [`normalize_generic_ledger`] (port of
//! `_normalize_generic_ledger_for_total_cost_budget`, cl-revenue-ops.py:
//! 8177-8195) and [`open_close_cost_visibility`] (port of
//! `_open_close_cost_visibility`, cl-revenue-ops.py:8198-8240) are pure
//! dict-reshaping functions over an already-fetched ledger blob -- real
//! ports, not stubs. [`crate::rpc_capex_status`]'s sibling growth-budget
//! math is also fully ported (`revops_analytics::growth::
//! compute_growth_budget_status`).
//!
//! Everything else this RPC reports is sourced from six DB/subprocess reads
//! that have NO Rust equivalent anywhere in this workspace (confirmed by
//! grep across `crates/`): `database.get_total_routing_revenue`,
//! `.get_opening_costs_since`, `.get_closure_costs_since`,
//! `.get_daily_rebalance_spend` (backing `_rebalance_liquidity_cost_
//! components`), `boltz_manager.get_boltz_cost_components` (backing
//! `_boltz_liquidity_cost_components`), `.get_spend_ledger_summary`, and
//! `.get_cost_evidence_coverage`. This builder therefore takes each of
//! those as an `Option` and only computes the values that transitively
//! depend on them when ALL of the needed pieces are present -- reporting a
//! category subtotal from only SOME of its components would look complete
//! while silently under-counting spend, which this project's honesty
//! convention forbids. Every field left `null` is listed by name in
//! `_phase1b_gaps`.

use revops_analytics::growth::{compute_growth_budget_status, GrowthBudgetInputs};
use serde_json::{json, Map, Value};

/// `wh = max(1, min(168, int(window_hours or 24)))` (cl-revenue-ops.py:
/// 8312-8313). Unlike [`crate::rpc_dashboard::parse_window_days`] (range
/// `[1,365]`, default 30) this RPC's window is hours, range `[1,168]`,
/// default 24 -- distinct enough from that sibling parser that sharing code
/// would obscure both ranges' provenance.
pub fn parse_window_hours(raw: Option<&Value>) -> Result<i64, Value> {
    let bad = || json!({"error": "window_hours must be an integer"});
    let parsed: i64 = match raw {
        None | Some(Value::Null) => 24,
        Some(Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                i
            } else if let Some(f) = n.as_f64() {
                f.trunc() as i64
            } else {
                return Err(bad());
            }
        }
        Some(Value::String(s)) => s.trim().parse::<i64>().map_err(|_| bad())?,
        Some(_) => return Err(bad()),
    };
    Ok(parsed.clamp(1, 168))
}

fn as_i64_or_zero(v: &Value, key: &str) -> i64 {
    v.get(key).and_then(Value::as_i64).unwrap_or(0)
}

const CANONICAL_LEDGER_CATEGORIES: [&str; 2] = ["channel_open", "channel_close"];

/// Port of `_normalize_generic_ledger_for_total_cost_budget`
/// (cl-revenue-ops.py:8177-8195): excludes canonical open/close spend
/// events from the generic ledger's budget-relevant `spent_24h_sats` (they
/// are separately, and already, counted via `open_cost_sats`/
/// `closure_cost_sats`), keeping the excluded amounts visible for
/// [`open_close_cost_visibility`].
pub fn normalize_generic_ledger(raw: &Value) -> Value {
    let mut normalized = raw.as_object().cloned().unwrap_or_default();

    let spent_by_category = normalized
        .get("spent_by_category")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    let mut counted = Map::new();
    let mut excluded = Map::new();
    for (category, amount) in &spent_by_category {
        let amount_int = amount.as_i64().unwrap_or(0);
        if CANONICAL_LEDGER_CATEGORIES.contains(&category.as_str()) {
            excluded.insert(category.clone(), json!(amount_int));
        } else {
            counted.insert(category.clone(), json!(amount_int));
        }
    }

    let raw_spent = as_i64_or_zero(raw, "spent_24h_sats");
    let counted_total: i64 = counted.values().filter_map(Value::as_i64).sum();

    normalized.insert("raw_spent_24h_sats".to_string(), json!(raw_spent));
    normalized.insert("spent_24h_sats".to_string(), json!(counted_total));
    normalized.insert(
        "counted_spent_categories".to_string(),
        Value::Object(counted),
    );
    normalized.insert(
        "excluded_spent_categories".to_string(),
        Value::Object(excluded),
    );
    normalized
        .entry("event_count_by_category")
        .or_insert_with(|| json!({}));
    normalized
        .entry("active_reservation_count_by_category")
        .or_insert_with(|| json!({}));
    Value::Object(normalized)
}

/// Port of `_open_close_cost_visibility` (cl-revenue-ops.py:8198-8240).
/// `generic_ledger` must already be [`normalize_generic_ledger`]'s output
/// (it reads `excluded_spent_categories`, `reserved_by_category`,
/// `event_count_by_category`, `active_reservation_count_by_category`).
pub fn open_close_cost_visibility(
    generic_ledger: &Value,
    open_cost_sats: i64,
    closure_cost_sats: i64,
) -> Value {
    let obj = |key: &str| {
        generic_ledger
            .get(key)
            .cloned()
            .unwrap_or_else(|| json!({}))
    };
    let excluded = obj("excluded_spent_categories");
    let reserved = obj("reserved_by_category");
    let event_counts = obj("event_count_by_category");
    let reservation_counts = obj("active_reservation_count_by_category");

    let mut pending_events: i64 = 0;
    if open_cost_sats <= 0 && as_i64_or_zero(&excluded, "channel_open") > 0 {
        pending_events += as_i64_or_zero(&event_counts, "channel_open").max(1);
    }
    if closure_cost_sats <= 0 && as_i64_or_zero(&excluded, "channel_close") > 0 {
        pending_events += as_i64_or_zero(&event_counts, "channel_close").max(1);
    }
    pending_events += as_i64_or_zero(&reservation_counts, "channel_open").max(0);
    pending_events += as_i64_or_zero(&reservation_counts, "channel_close").max(0);

    json!({
        "canonical_open_cost_available": open_cost_sats > 0,
        "canonical_close_cost_available": closure_cost_sats > 0,
        "pending_open_close_spend_events": pending_events,
        "excluded_from_generic_totals_to_avoid_double_count": true,
        "excluded_open_close_spend_sats":
            as_i64_or_zero(&excluded, "channel_open") + as_i64_or_zero(&excluded, "channel_close"),
        "reserved_open_close_sats":
            as_i64_or_zero(&reserved, "channel_open") + as_i64_or_zero(&reserved, "channel_close"),
    })
}

/// `covered_hours`/`coverage_status` (cl-revenue-ops.py:8265-8285): honest
/// unknown when there is no coverage evidence, never a fabricated
/// "complete". `coverage` is the raw `get_cost_evidence_coverage()` dict
/// (`{"covered_hours": <num>, "coverage_status": <str>?}`), already fetched
/// by the caller.
fn coverage_fields(coverage: Option<&Value>, window_hours: i64) -> (Value, &'static str) {
    let Some(raw_covered) = coverage
        .and_then(|c| c.get("covered_hours"))
        .and_then(Value::as_f64)
    else {
        return (Value::Null, "unknown");
    };
    let capped = raw_covered.min(window_hours as f64);
    let covered_json = if capped == capped.trunc() {
        json!(capped as i64)
    } else {
        json!(capped)
    };
    let status = coverage
        .and_then(|c| c.get("coverage_status"))
        .and_then(Value::as_str);
    let status: &'static str = match status {
        Some("complete") => "complete",
        Some("partial") => "partial",
        Some(_) | None => {
            if capped >= window_hours as f64 {
                "complete"
            } else {
                "partial"
            }
        }
    };
    (covered_json, status)
}

/// Already-fetched inputs to [`build_total_cost_budget`]. Every `Option`
/// field mirrors one live DB/subprocess read Python performs inline; `None`
/// means that read has no Rust caller yet (see module doc comment).
#[derive(Default)]
pub struct TotalCostBudgetInputs<'a> {
    pub now: i64,
    pub daily_budget_sats: i64,
    pub growth_enabled: bool,
    pub growth_earned_fraction: f64,
    pub growth_experiment_fraction: f64,
    pub growth_max_extra_sats: i64,
    pub growth_hard_ceiling_sats: i64,
    pub fleet_prior: Option<&'a Value>,
    pub revenue_sats: Option<i64>,
    pub open_cost_sats: Option<i64>,
    pub closure_cost_sats: Option<i64>,
    /// Raw `_rebalance_liquidity_cost_components` shape: must contain
    /// `spent_24h_sats`/`reserved_24h_sats` to be usable.
    pub rebalance_component: Option<&'a Value>,
    /// Raw `_boltz_liquidity_cost_components` shape.
    pub boltz_component: Option<&'a Value>,
    /// Raw (pre-[`normalize_generic_ledger`]) `get_spend_ledger_summary` shape.
    pub generic_ledger_raw: Option<&'a Value>,
    /// Raw `get_cost_evidence_coverage` shape.
    pub coverage_raw: Option<&'a Value>,
}

/// Build the `revenue-total-cost-budget` response body. `window_hours` is
/// the already-[`parse_window_hours`]-clamped value.
pub fn build_total_cost_budget(window_hours: i64, i: &TotalCostBudgetInputs) -> Value {
    let since_timestamp = i.now - window_hours * 3600;
    let generic_ledger = i.generic_ledger_raw.map(normalize_generic_ledger);

    let rebalance_spent = i
        .rebalance_component
        .map(|v| as_i64_or_zero(v, "spent_24h_sats"));
    let rebalance_reserved = i
        .rebalance_component
        .map(|v| as_i64_or_zero(v, "reserved_24h_sats"));
    let boltz_spent = i
        .boltz_component
        .map(|v| as_i64_or_zero(v, "spent_24h_sats"));
    let boltz_reserved = i
        .boltz_component
        .map(|v| as_i64_or_zero(v, "reserved_24h_sats"));
    let ledger_spent = generic_ledger
        .as_ref()
        .map(|v| as_i64_or_zero(v, "spent_24h_sats"));
    let ledger_reserved = generic_ledger
        .as_ref()
        .map(|v| as_i64_or_zero(v, "reserved_24h_sats"));
    let ledger_rebalance_reserved = generic_ledger.as_ref().map(|v| {
        v.get("reserved_by_category")
            .map(|r| as_i64_or_zero(r, "rebalance"))
            .unwrap_or(0)
    });

    // Python sums every category unconditionally, defaulting a missing
    // component to 0 -- but a Rust `None` here means "never fetched", not
    // "fetched as zero". Silently treating an unfetched component as zero
    // would under-report actual spend while looking like a real total, so
    // the combined totals (and everything derived from them) stay `null`
    // unless every category is genuinely known.
    let all_categories_known = i.open_cost_sats.is_some()
        && i.closure_cost_sats.is_some()
        && rebalance_spent.is_some()
        && boltz_spent.is_some()
        && ledger_spent.is_some();
    let all_reserved_known = rebalance_reserved.is_some()
        && boltz_reserved.is_some()
        && ledger_reserved.is_some()
        && ledger_rebalance_reserved.is_some();

    let actual_by_category = all_categories_known.then(|| {
        json!({
            "rebalance": rebalance_spent.unwrap(),
            "boltz": boltz_spent.unwrap(),
            "open": i.open_cost_sats.unwrap(),
            "close": i.closure_cost_sats.unwrap(),
            "ledger": ledger_spent.unwrap(),
        })
    });
    let actual_total = all_categories_known.then(|| {
        rebalance_spent.unwrap().max(0)
            + boltz_spent.unwrap().max(0)
            + i.open_cost_sats.unwrap().max(0)
            + i.closure_cost_sats.unwrap().max(0)
            + ledger_spent.unwrap().max(0)
    });

    let reserved_by_category = all_reserved_known.then(|| {
        let ledger_only = (ledger_reserved.unwrap() - ledger_rebalance_reserved.unwrap()).max(0);
        json!({
            "rebalance": rebalance_reserved.unwrap(),
            "boltz": boltz_reserved.unwrap(),
            "ledger": ledger_only,
        })
    });
    let reserved_total = reserved_by_category.as_ref().map(|v| {
        as_i64_or_zero(v, "rebalance").max(0)
            + as_i64_or_zero(v, "boltz").max(0)
            + as_i64_or_zero(v, "ledger").max(0)
    });

    let net_profit_sats = match (i.revenue_sats, actual_total) {
        (Some(rev), Some(spent)) => Some(rev - spent),
        _ => None,
    };

    let growth_budget = match (net_profit_sats, actual_total, reserved_total) {
        (Some(net_profit), Some(spent), Some(reserved)) => {
            let inputs = GrowthBudgetInputs {
                base_budget_sats: i.daily_budget_sats.max(0),
                net_profit_sats: net_profit,
                actual_spent_sats: spent,
                reserved_sats: reserved,
                enabled: i.growth_enabled,
                earned_fraction: i.growth_earned_fraction,
                growth_fraction: i.growth_experiment_fraction,
                growth_max_extra_sats: i.growth_max_extra_sats,
                hard_ceiling_sats: if i.growth_hard_ceiling_sats > 0 {
                    i.growth_hard_ceiling_sats
                } else {
                    i.daily_budget_sats.max(0)
                },
            };
            Some(compute_growth_budget_status(&inputs, i.fleet_prior))
        }
        _ => None,
    };

    let mode = growth_budget.as_ref().map(|g| g.mode).unwrap_or("fixed");
    let effective_budget_sats = growth_budget
        .as_ref()
        .map(|g| g.effective_budget_sats)
        .unwrap_or(i.daily_budget_sats.max(0));
    let remaining_sats = growth_budget
        .as_ref()
        .map(|g| g.remaining_sats)
        .or_else(|| match (actual_total, reserved_total) {
            (Some(spent), Some(reserved)) => {
                Some((effective_budget_sats - spent - reserved).max(0))
            }
            _ => None,
        });

    let open_close_visibility = match (&generic_ledger, i.open_cost_sats, i.closure_cost_sats) {
        (Some(ledger), Some(open_cost), Some(close_cost)) => {
            Some(open_close_cost_visibility(ledger, open_cost, close_cost))
        }
        _ => None,
    };

    let (covered_hours, coverage_status) = coverage_fields(i.coverage_raw, window_hours);

    let mut gaps: Vec<&'static str> = Vec::new();
    if i.revenue_sats.is_none() {
        gaps.push("revenue_sats");
    }
    if actual_by_category.is_none() {
        gaps.push("actual_spent_sats");
        gaps.push("actual_spent_by_category");
        gaps.push("net_profit_sats_after_costs");
    }
    if reserved_by_category.is_none() {
        gaps.push("reserved_sats");
        gaps.push("reserved_by_category");
    }
    if growth_budget.is_none() {
        gaps.push("growth_budget");
        gaps.push("mode");
        gaps.push("effective_budget_sats");
        gaps.push("remaining_sats");
    }
    if open_close_visibility.is_none() {
        gaps.push("open_close_cost_visibility");
    }
    if i.rebalance_component.is_none() {
        gaps.push("components.rebalance");
    }
    if i.boltz_component.is_none() {
        gaps.push("components.boltz");
    }
    if generic_ledger.is_none() {
        gaps.push("components.generic_ledger");
    }
    if i.open_cost_sats.is_none() {
        gaps.push("components.open_cost_sats");
    }
    if i.closure_cost_sats.is_none() {
        gaps.push("components.closure_cost_sats");
    }
    if covered_hours.is_null() {
        gaps.push("coverage_hours");
        gaps.push("covered_hours");
    }

    json!({
        "source": "total_cost_budget",
        "timestamp": i.now,
        "generated_at": i.now,
        "ttl_seconds": 1800,
        "window_hours": window_hours,
        "coverage_hours": covered_hours.clone(),
        "covered_hours": covered_hours,
        "coverage_status": coverage_status,
        "since_timestamp": since_timestamp,
        "mode": mode,
        "daily_budget_sats": i.daily_budget_sats.max(0),
        "effective_budget_sats": effective_budget_sats,
        "revenue_sats": i.revenue_sats,
        "actual_spent_sats": actual_total,
        "reserved_sats": reserved_total,
        "remaining_sats": remaining_sats,
        "net_profit_sats_after_costs": net_profit_sats,
        "growth_budget": growth_budget.map(|g| Value::Object(
            g.to_ordered_pairs().into_iter().map(|(k, v)| (k.to_string(), v)).collect()
        )),
        "actual_spent_by_category": actual_by_category,
        "reserved_by_category": reserved_by_category,
        "open_close_cost_visibility": open_close_visibility,
        "components": {
            "rebalance": i.rebalance_component,
            "boltz": i.boltz_component,
            "generic_ledger": generic_ledger,
            "open_cost_sats": i.open_cost_sats,
            "closure_cost_sats": i.closure_cost_sats,
        },
        "_phase1b_gaps": gaps,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_hours_defaults_to_24_and_clamps_to_168() {
        assert_eq!(parse_window_hours(None).unwrap(), 24);
        assert_eq!(parse_window_hours(Some(&json!(9999))).unwrap(), 168);
        assert_eq!(parse_window_hours(Some(&json!(0))).unwrap(), 1);
        assert!(parse_window_hours(Some(&json!("nope"))).is_err());
    }

    #[test]
    fn normalize_generic_ledger_excludes_canonical_categories_from_spent_total() {
        let raw = json!({
            "spent_24h_sats": 500,
            "spent_by_category": {"channel_open": 300, "rebalance": 200},
        });
        let n = normalize_generic_ledger(&raw);
        assert_eq!(n["raw_spent_24h_sats"], 500);
        assert_eq!(n["spent_24h_sats"], 200, "channel_open must be excluded");
        assert_eq!(n["excluded_spent_categories"]["channel_open"], 300);
        assert_eq!(n["counted_spent_categories"]["rebalance"], 200);
    }

    #[test]
    fn open_close_visibility_counts_pending_events_only_when_cost_unavailable() {
        let ledger = json!({
            "excluded_spent_categories": {"channel_open": 300, "channel_close": 0},
            "reserved_by_category": {"channel_open": 10, "channel_close": 20},
            "event_count_by_category": {"channel_open": 2},
            "active_reservation_count_by_category": {},
        });
        let v = open_close_cost_visibility(&ledger, 0, 3000);
        assert_eq!(v["canonical_open_cost_available"], false);
        assert_eq!(v["canonical_close_cost_available"], true);
        // open_cost_sats<=0 AND excluded.channel_open>0 -> pending += event_count (2)
        assert_eq!(v["pending_open_close_spend_events"], 2);
        assert_eq!(v["excluded_open_close_spend_sats"], 300);
        assert_eq!(v["reserved_open_close_sats"], 30);

        // Control: when the canonical cost IS available, no pending events
        // are added for that side even though the excluded bucket is nonzero.
        let v2 = open_close_cost_visibility(&ledger, 5000, 3000);
        assert_eq!(
            v2["pending_open_close_spend_events"], 0,
            "canonical open cost available must suppress the open pending count"
        );
    }

    #[test]
    fn coverage_defaults_to_unknown_without_fabricating_complete() {
        let (hours, status) = coverage_fields(None, 24);
        assert_eq!(hours, Value::Null);
        assert_eq!(status, "unknown");
    }

    #[test]
    fn coverage_caps_at_window_hours_and_infers_status() {
        let coverage = json!({"covered_hours": 999.0});
        let (hours, status) = coverage_fields(Some(&coverage), 24);
        assert_eq!(
            hours,
            json!(24),
            "covered_hours must cap at the window, never exceed it"
        );
        assert_eq!(status, "complete");

        let partial = json!({"covered_hours": 6.5});
        let (hours2, status2) = coverage_fields(Some(&partial), 24);
        assert_eq!(hours2, json!(6.5));
        assert_eq!(status2, "partial");
    }

    fn base_inputs() -> TotalCostBudgetInputs<'static> {
        TotalCostBudgetInputs {
            now: 1_700_000_000,
            daily_budget_sats: 100_000,
            growth_enabled: false,
            growth_earned_fraction: 0.25,
            growth_experiment_fraction: 0.10,
            growth_max_extra_sats: 0,
            growth_hard_ceiling_sats: 0,
            ..Default::default()
        }
    }

    #[test]
    fn missing_components_gap_every_dependent_field_not_just_their_own() {
        let inputs = base_inputs();
        let v = build_total_cost_budget(24, &inputs);
        assert_eq!(v["revenue_sats"], Value::Null);
        assert_eq!(v["actual_spent_sats"], Value::Null);
        assert_eq!(v["growth_budget"], Value::Null);
        assert_eq!(
            v["mode"], "fixed",
            "mode falls back to the honest fixed default"
        );
        let gaps = v["_phase1b_gaps"].as_array().unwrap();
        assert!(gaps.iter().any(|g| g == "actual_spent_by_category"));
        assert!(gaps.iter().any(|g| g == "growth_budget"));
    }

    #[test]
    fn full_components_compute_a_real_total_not_a_gap() {
        let rebalance = json!({"spent_24h_sats": 1000, "reserved_24h_sats": 100});
        let boltz = json!({"spent_24h_sats": 2000, "reserved_24h_sats": 200});
        let ledger_raw = json!({
            "spent_24h_sats": 500,
            "reserved_24h_sats": 150,
            "spent_by_category": {"rebalance": 500},
            "reserved_by_category": {"rebalance": 100},
        });
        let mut inputs = base_inputs();
        inputs.revenue_sats = Some(10_000);
        inputs.open_cost_sats = Some(0);
        inputs.closure_cost_sats = Some(0);
        inputs.rebalance_component = Some(&rebalance);
        inputs.boltz_component = Some(&boltz);
        inputs.generic_ledger_raw = Some(&ledger_raw);

        let v = build_total_cost_budget(24, &inputs);
        // rebalance(1000) + boltz(2000) + open(0) + close(0) + ledger(500,
        // category "rebalance" is not canonical open/close so it is NOT
        // excluded by normalize -> fully counted).
        assert_eq!(v["actual_spent_sats"], 3500);
        // reserved: rebalance(100) + boltz(200) + ledger(150 total minus its
        // own 100 already counted under "rebalance" = 50) = 350.
        assert_eq!(v["reserved_sats"], 350);
        assert_eq!(v["net_profit_sats_after_costs"], 6500);
        assert_ne!(v["growth_budget"], Value::Null);
        let gaps = v["_phase1b_gaps"].as_array().unwrap();
        assert!(
            !gaps.iter().any(|g| g == "actual_spent_by_category"),
            "fully-known components must not be gap-listed: {gaps:?}"
        );
    }
}
