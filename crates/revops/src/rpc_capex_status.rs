//! Pure response builder for `revenue-capex-status`.
//!
//! Port of `revenue_capex_status` (cl-revenue-ops.py:7714-7777), the
//! success path only: `capex_engine.compute_allocations()` ->
//! `CapexAllocations` (py `modules/capex_budget.py:82-119`), serialized 1:1.
//! The two Python error paths (`"Capex engine not initialized"` /
//! `"Allocation failed: {e}"`) are the caller's concern (this crate's
//! `compute_allocations` is infallible given a `CapexEvidence`/`CapexConfig`
//! pair, and "no engine" is a wiring precondition, not a shape this builder
//! renders) and are therefore NOT reproduced here.
//!
//! The `compute_allocations` kernel itself (`crates/revops-capital/src/
//! capex.rs`) is fully ported and fixture-verified against the real Python
//! engine (`crates/revops-capital/tests/capex.rs`), so every field below is
//! a real computed value -- no `_phase1b_gaps` in this builder (`timestamp`/
//! `generated_at` are the caller-supplied wall clock, cl-revenue-ops.py:
//! 7740-7743, not a stub). What is NOT ported, and is out of scope for a
//! pure builder, is the Python
//! *evidence-gathering* half (`analyze_all_channels()`, bleeder status,
//! `get_total_capex_by_channel(30)`, `get_spend_ledger_summary(30)`,
//! confirmed on-chain balance, capital-efficiency snapshot) that produces
//! the `CapexEvidence` this builder's caller must supply, and the Python
//! `data_service.datastore_push(["revenue","capex-summary"], ...)` side
//! effect (not part of the returned value in Python either). See
//! `RPC_BATCH_B.md` for the fetch list a real handler needs.

use revops_capital::capex::CapexAllocations;
use serde_json::{json, Value};

/// Build the `revenue-capex-status` response body from an already-computed
/// [`CapexAllocations`] snapshot. `now` is the caller-supplied Unix
/// timestamp (py `int(time.time())`, cl-revenue-ops.py:7719-7720 sets both
/// `timestamp` and `generated_at` to the same value) -- matching
/// `rpc_report::build_report`'s `generated_at` convention rather than
/// reading the clock inside this pure builder.
pub fn build_capex_status(alloc: &CapexAllocations, now: i64) -> Value {
    let mut channels = serde_json::Map::new();
    for (channel_id, budget) in &alloc.channel_budgets {
        channels.insert(
            channel_id.clone(),
            json!({
                "budget_sats": budget.budget_sats(),
                "tier": budget.tier.as_str(),
                "tier_ppm": budget.tier_ppm,
                "priority_class": budget.priority_class.as_str(),
            }),
        );
    }

    let by_priority = |p: revops_capital::capex::PriorityClass| -> i64 {
        alloc
            .allocated_by_priority_msat
            .get(&p)
            .copied()
            .map(|msat| revops_core::msat::base_to_sats_ceil(msat.max(0) as u64) as i64)
            .unwrap_or(0)
    };
    use revops_capital::capex::PriorityClass::*;

    json!({
        "timestamp": now,
        "generated_at": now,
        "ttl_seconds": 1800,
        "status": "ok",
        "priority_class": alloc.priority_class.as_str(),
        "global_envelope_sats": alloc.global_envelope_sats(),
        "fleet_exploration_budget_sats": alloc.fleet_exploration_budget_sats(),
        "tactical_budget_sats": alloc.tactical_budget_sats(),
        "total_fleet_contribution_sats": alloc.total_fleet_contribution_sats(),
        "allocated_by_priority_sats": {
            "defensive": by_priority(Defensive),
            "preservation": by_priority(Preservation),
            "operational": by_priority(Operational),
            "growth": by_priority(Growth),
        },
        "channel_count": alloc.channel_budgets.len(),
        "channels": Value::Object(channels),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use revops_capital::capex::{ChannelCapexBudget, PriorityClass, Tier};
    use std::collections::BTreeMap;

    fn alloc_with_one_channel() -> CapexAllocations {
        let mut channel_budgets = BTreeMap::new();
        channel_budgets.insert(
            "123x456x0".to_string(),
            ChannelCapexBudget {
                channel_id: "123x456x0".to_string(),
                budget_msat: 5_500_000,
                tier: Tier::Active,
                tier_ppm: 250,
                priority_class: PriorityClass::Operational,
                success_rate_30d: Some(0.9),
                roi_multiplier: 1.2,
            },
        );
        let mut allocated_by_priority_msat = BTreeMap::new();
        allocated_by_priority_msat.insert(PriorityClass::Operational, 5_500_000);
        allocated_by_priority_msat.insert(PriorityClass::Growth, 1_000);

        CapexAllocations {
            priority_class: PriorityClass::Operational,
            global_envelope_msat: 10_000_000,
            channel_budgets,
            fleet_exploration_budget_msat: 2_000_000,
            tactical_budget_msat: 1_000_000,
            allocated_by_priority_msat,
            total_fleet_contribution_msat: 3_000_000,
            db_degraded: false,
        }
    }

    #[test]
    fn per_channel_fields_match_python_shape() {
        let alloc = alloc_with_one_channel();
        let v = build_capex_status(&alloc, 1_700_000_000);
        let ch = &v["channels"]["123x456x0"];
        // msat 5_500_000 ceils to 5_500 sats exactly (no remainder) -- the
        // control below (5_500_001) proves ceiling, not truncation.
        assert_eq!(ch["budget_sats"], 5_500);
        assert_eq!(ch["tier"], "active");
        assert_eq!(ch["tier_ppm"], 250);
        assert_eq!(ch["priority_class"], "operational");
        assert_eq!(v["channel_count"], 1);
    }

    #[test]
    fn budget_sats_rounds_up_not_down() {
        let mut alloc = alloc_with_one_channel();
        alloc
            .channel_budgets
            .get_mut("123x456x0")
            .unwrap()
            .budget_msat = 5_500_001;
        let v = build_capex_status(&alloc, 1_700_000_000);
        assert_eq!(
            v["channels"]["123x456x0"]["budget_sats"], 5_501,
            "1 extra msat must ceil the sats value up, not floor it"
        );
    }

    #[test]
    fn allocated_by_priority_sats_covers_all_four_classes_defaulting_absent_to_zero() {
        let alloc = alloc_with_one_channel();
        let v = build_capex_status(&alloc, 1_700_000_000);
        let by_prio = &v["allocated_by_priority_sats"];
        assert_eq!(by_prio["operational"], 5_500);
        assert_eq!(by_prio["growth"], 1);
        // Control: classes never inserted into allocated_by_priority_msat
        // must still appear, defaulted to zero -- not silently dropped.
        assert_eq!(by_prio["defensive"], 0);
        assert_eq!(by_prio["preservation"], 0);
    }

    #[test]
    fn timestamp_and_generated_at_both_echo_the_caller_supplied_clock() {
        let v = build_capex_status(&alloc_with_one_channel(), 1_700_000_042);
        assert_eq!(v["timestamp"], 1_700_000_042);
        assert_eq!(v["generated_at"], 1_700_000_042);
        assert_eq!(v["ttl_seconds"], 1800);
        assert_eq!(v["status"], "ok");
    }
}
