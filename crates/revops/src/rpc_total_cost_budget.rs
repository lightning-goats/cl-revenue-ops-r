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
//! The evidence reads all have live Rust equivalents as of Task 66 slice 1
//! (they did not when this builder was first written):
//! `queries::total_routing_revenue_msat`, `queries::opening_costs_since`,
//! `queries::closure_costs_since`, `queries::rebalance_spend_component`
//! (backing `_rebalance_liquidity_cost_components` via
//! [`rebalance_component_value`]), `rpc_boltz_ops::
//! total_cost_boltz_component` (backing `_boltz_liquidity_cost_
//! components`), `queries::spend_ledger_aggregates` +
//! `active_spend_reservations` (backing `get_spend_ledger_summary`), and
//! `queries::cost_evidence_coverage`. [`total_cost_budget_response`] wires
//! all of them, so in production every input is `Some` and `_phase1b_gaps`
//! is empty. The builder keeps its `Option` inputs: a caller that has not
//! fetched a pipeline passes `None` and every transitively dependent value
//! stays `null` and gap-listed by name -- reporting a category subtotal
//! from only SOME of its components would look complete while silently
//! under-counting spend, which this project's honesty convention forbids.

use revops_analytics::growth::{compute_growth_budget_status, GrowthBudgetInputs};
use revops_db::actor::DbHandle;
use revops_db::queries;
use serde_json::{json, Map, Value};

use crate::rpc_params::{is_truthy_py, python_int};

/// `wh = max(1, min(168, int(window_hours or 24)))` (cl-revenue-ops.py:
/// 8312-8313). Python applies `or 24` before `int()`, so every falsy JSON
/// value defaults to 24; truthy values use the shared exact `int()` port.
pub fn parse_window_hours(raw: Option<&Value>) -> Result<i64, Value> {
    let default = Value::Number(24.into());
    let value = raw.filter(|value| is_truthy_py(value)).unwrap_or(&default);
    let parsed = python_int(value).map_err(|error| json!({"error": error}))?;
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

/// The rebalance component of the budget response, port of
/// `_rebalance_liquidity_cost_components` (cl-revenue-ops.py:8111-8134):
/// the windowed DB read's four values on the available path, Python's
/// exception arm (`available: false`, error text, honest zeros) when the
/// read failed. Python's zeros here COUNT toward the category totals, so
/// the caller passes this dict as a present component, never `None`.
pub fn rebalance_component_value(
    outcome: Result<&revops_db::queries::RebalanceSpendComponent, String>,
    window_hours: i64,
) -> Value {
    match outcome {
        Ok(component) => json!({
            "source": "rebalance",
            "available": true,
            "window_hours": window_hours,
            "spent_24h_sats": component.total_spent_sats,
            "reserved_24h_sats": component.total_reserved_sats,
            "job_count": component.job_count,
            "success_count": component.success_count,
        }),
        Err(error) => json!({
            "source": "rebalance",
            "available": false,
            "error": error,
            "spent_24h_sats": 0,
            "reserved_24h_sats": 0,
        }),
    }
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
    // Gap only when the coverage pipeline was never FETCHED. A wired
    // pipeline measuring "unknown" (no evidence rows) is Python's own
    // honest null, not a Rust wiring gap.
    if i.coverage_raw.is_none() {
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

/// Everything the assembled response needs beyond the DB itself. The
/// Boltz component arrives pre-computed (`rpc_boltz_ops::
/// total_cost_boltz_component` needs the Boltz deps, which live in
/// `main.rs`); the config scalars arrive pre-resolved for the same reason.
/// Python's equivalents come off the in-memory `config.snapshot()`.
pub struct TotalCostBudgetSources<'a> {
    pub db: Option<&'a DbHandle>,
    pub boltz_component: Value,
    pub daily_budget_sats: i64,
    pub growth_enabled: bool,
    pub growth_earned_fraction: f64,
    pub growth_experiment_fraction: f64,
    pub growth_max_extra_sats: i64,
    pub growth_hard_ceiling_sats: i64,
    pub now: i64,
}

/// Port of `_compute_total_cost_budget_status`'s evidence gathering
/// (cl-revenue-ops.py:8347-8482), feeding [`build_total_cost_budget`].
/// `window_hours` must already be [`parse_window_hours`]-clamped.
///
/// Error semantics mirror Python arm-for-arm:
/// - no DB -> the exact `{"error": "Plugin not initialized"}` 1-key arm;
/// - a rebalance-component read failure -> that component's
///   `available: false` dict (caught inside the provider);
/// - a coverage read failure -> measured-unknown nulls (caught);
/// - ledger/revenue/open/close read failures -> the whole-response
///   `{"error": ...}` arm (Python leaves these unguarded, so the RPC's
///   outer `except` answers).
///
/// DELIBERATE DEFERRAL: Python starts with a best-effort
/// `cleanup_stale_spend_reservations` write (cl-revenue-ops.py:8353-8360).
/// This port's writes go through the sealed mutation owner only; the
/// cleanup lands with the `revenue-spend-release-stale` slice (same
/// operation) rather than as an inline write on a read path. Until then a
/// stale reservation keeps counting as reserved — the conservative
/// direction (budget under-spends, never over-spends).
pub async fn total_cost_budget_response(window_hours: i64, s: TotalCostBudgetSources<'_>) -> Value {
    let Some(handle) = s.db else {
        return json!({"error": "Plugin not initialized"});
    };
    let since = s.now - window_hours * 3600;

    let rebalance = queries::rebalance_spend_component(handle, window_hours, s.now)
        .await
        .map_err(|error| error.to_string());
    let rebalance_component =
        rebalance_component_value(rebalance.as_ref().map_err(Clone::clone), window_hours);

    let aggregates = match queries::spend_ledger_aggregates(handle, window_hours, s.now).await {
        Ok(aggregates) => aggregates,
        Err(error) => return json!({"error": error.to_string()}),
    };
    // include_reservations=True with Python's default limit of 50
    // (cl-revenue-ops.py:8367, database.py:4487).
    let reservations =
        match queries::active_spend_reservations(handle, window_hours, 50, s.now).await {
            Ok(rows) => rows,
            Err(error) => return json!({"error": error.to_string()}),
        };
    let generic_ledger_raw = crate::rpc_spend_ledger::build_spend_ledger(
        window_hours,
        s.now,
        &aggregates,
        Some(&reservations),
    );

    let revenue_msat = match queries::total_routing_revenue_msat(handle, since, s.now).await {
        Ok(msat) => msat,
        Err(error) => return json!({"error": error.to_string()}),
    };
    let open_cost_sats = match queries::opening_costs_since(handle, since).await {
        Ok(sats) => sats,
        Err(error) => return json!({"error": error.to_string()}),
    };
    let closure_cost_sats = match queries::closure_costs_since(handle, since).await {
        Ok(sats) => sats,
        Err(error) => return json!({"error": error.to_string()}),
    };

    // Python wraps this read in try/except -> unknown, never an error arm.
    let coverage_raw = match queries::cost_evidence_coverage(handle, window_hours, s.now).await {
        Ok(coverage) => json!({
            "covered_hours": coverage.covered_hours,
            "coverage_status": coverage.coverage_status,
        }),
        Err(_) => Value::Null,
    };

    build_total_cost_budget(
        window_hours,
        &TotalCostBudgetInputs {
            now: s.now,
            daily_budget_sats: s.daily_budget_sats,
            growth_enabled: s.growth_enabled,
            growth_earned_fraction: s.growth_earned_fraction,
            growth_experiment_fraction: s.growth_experiment_fraction,
            growth_max_extra_sats: s.growth_max_extra_sats,
            growth_hard_ceiling_sats: s.growth_hard_ceiling_sats,
            fleet_prior: None,
            // Python: `revenue_msat // 1000` — floor division, ported
            // exactly (i64 `/` truncates toward zero instead).
            revenue_sats: Some(revenue_msat.div_euclid(1000)),
            open_cost_sats: Some(open_cost_sats),
            closure_cost_sats: Some(closure_cost_sats),
            rebalance_component: Some(&rebalance_component),
            boltz_component: Some(&s.boltz_component),
            generic_ledger_raw: Some(&generic_ledger_raw),
            coverage_raw: Some(&coverage_raw),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_hours_matches_python_or_default_then_int_then_clamp() {
        assert_eq!(parse_window_hours(None).unwrap(), 24);
        for falsy in [
            json!(null),
            json!(false),
            json!(0),
            json!(""),
            json!([]),
            json!({}),
        ] {
            assert_eq!(parse_window_hours(Some(&falsy)).unwrap(), 24, "{falsy}");
        }
        assert_eq!(parse_window_hours(Some(&json!(true))).unwrap(), 1);
        assert_eq!(parse_window_hours(Some(&json!(9999))).unwrap(), 168);
        assert_eq!(parse_window_hours(Some(&json!(-1))).unwrap(), 1);
        assert_eq!(parse_window_hours(Some(&json!("0"))).unwrap(), 1);
        assert_eq!(parse_window_hours(Some(&json!(" 45 "))).unwrap(), 45);
        assert_eq!(
            parse_window_hours(Some(&json!("nope"))).unwrap_err(),
            json!({"error": "invalid literal for int() with base 10: \x27nope\x27"})
        );
        assert_eq!(
            parse_window_hours(Some(&json!({"not": "empty"}))).unwrap_err(),
            json!({"error": "int() argument must be a string, a bytes-like object or a real number, not \x27dict\x27"})
        );
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
    fn rebalance_component_value_matches_pythons_available_dict() {
        let component = revops_db::queries::RebalanceSpendComponent {
            total_spent_sats: 1000,
            total_reserved_sats: 140,
            job_count: 4,
            success_count: 2,
        };
        assert_eq!(
            rebalance_component_value(Ok(&component), 24),
            json!({
                "source": "rebalance",
                "available": true,
                "window_hours": 24,
                "spent_24h_sats": 1000,
                "reserved_24h_sats": 140,
                "job_count": 4,
                "success_count": 2,
            })
        );
    }

    #[test]
    fn rebalance_component_value_error_arm_is_unavailable_zeros() {
        assert_eq!(
            rebalance_component_value(Err("db actor gone".to_string()), 24),
            json!({
                "source": "rebalance",
                "available": false,
                "error": "db actor gone",
                "spent_24h_sats": 0,
                "reserved_24h_sats": 0,
            })
        );
    }

    /// A WIRED coverage pipeline that measures "unknown" (empty evidence)
    /// is Python's own honest answer, not a Rust wiring gap — the gap
    /// entries must fire only when `coverage_raw` was never fetched.
    #[test]
    fn measured_unknown_coverage_is_not_a_wiring_gap() {
        let coverage = json!({"covered_hours": null, "coverage_status": "unknown"});
        let mut inputs = base_inputs();
        inputs.coverage_raw = Some(&coverage);
        let v = build_total_cost_budget(24, &inputs);
        assert_eq!(v["covered_hours"], Value::Null);
        assert_eq!(v["coverage_status"], "unknown");
        let gaps = v["_phase1b_gaps"].as_array().unwrap();
        assert!(
            !gaps
                .iter()
                .any(|g| g == "coverage_hours" || g == "covered_hours"),
            "measured unknown must not be declared as an unwired pipeline: {gaps:?}"
        );
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
