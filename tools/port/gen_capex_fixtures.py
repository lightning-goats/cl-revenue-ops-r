#!/usr/bin/env python3
"""Generate real-Python fixtures for the CapexBudgetEngine Rust port.

Runs the ACTUAL modules/capex_budget.py (imported unmodified from
/home/sat/bin/cl_revenue_ops) against constructed scenarios, and dumps
input + output as JSON so the Rust port can be tested against real
Python behavior rather than hand-derived expectations.
"""
import json
import sys
import types

sys.path.insert(0, "/home/sat/bin/cl_revenue_ops")

from modules.capex_budget import CapexBudgetEngine  # noqa: E402

MSAT_PER_SAT = 1000


def revenue(total_contribution_msat=0, fees_earned_msat=0, sourced_fee_contribution_msat=0,
            total_forward_count=0):
    return types.SimpleNamespace(
        total_contribution_msat=total_contribution_msat,
        fees_earned_msat=fees_earned_msat,
        sourced_fee_contribution_msat=sourced_fee_contribution_msat,
        total_forward_count=total_forward_count,
    )


def prof(classification="break_even", capacity_sats=1_000_000, days_open=30,
          marginal_roi=0.0, marginal_roi_reliable=True, channel_role=None,
          window_30d_available=False, contribution_30d_msat=0, fees_earned_30d_msat=0,
          **revenue_kwargs):
    return types.SimpleNamespace(
        classification=classification,
        capacity_sats=capacity_sats,
        days_open=days_open,
        marginal_roi=marginal_roi,
        marginal_roi_reliable=marginal_roi_reliable,
        channel_role=channel_role,
        window_30d_available=window_30d_available,
        contribution_30d_msat=contribution_30d_msat,
        fees_earned_30d_msat=fees_earned_30d_msat,
        revenue=revenue(**revenue_kwargs),
    )


class StubDatabase:
    def __init__(self, *, capex_by_channel=None, spend_summary=None,
                 onchain_sats=2_000_000, success_rates=None, fail_capex=False,
                 fail_spend_summary=False):
        self.capex_by_channel = capex_by_channel or {}
        self.spend_summary = spend_summary if spend_summary is not None else {
            "spent_by_category": {}, "reserved_by_category": {}
        }
        self.onchain_sats = onchain_sats
        self.success_rates = success_rates or {}
        self.fail_capex = fail_capex
        self.fail_spend_summary = fail_spend_summary
        self.spend_events = []
        self.reservations = {}

    def get_total_capex_by_channel(self, window_days):
        if self.fail_capex:
            raise RuntimeError("db error")
        return self.capex_by_channel

    def get_spend_ledger_summary(self, window_hours):
        if self.fail_spend_summary:
            raise RuntimeError("db error")
        return self.spend_summary

    def get_confirmed_onchain_sats(self):
        return self.onchain_sats

    def get_channel_rebalance_success_rate(self, ch_id, days):
        return self.success_rates.get(ch_id)

    def record_spend_event(self, **kwargs):
        self.spend_events.append(kwargs)
        return True

    def reserve_spend(self, **kwargs):
        rid = kwargs["reservation_id"]
        self.reservations[rid] = kwargs
        return True

    def release_spend_reservation(self, reservation_id):
        return self.reservations.pop(reservation_id, None) is not None


class StubBleeder:
    def __init__(self, classification):
        self.classification = classification


class StubProfitability:
    def __init__(self, database, channels, bleeders=None):
        self.database = database
        self._channels = channels
        self._bleeders = bleeders or {}

    def analyze_all_channels(self):
        return self._channels

    def get_bleeder_status(self, ch_id):
        return self._bleeders.get(ch_id)


class ChannelEff:
    def __init__(self, is_dead_capital=False, rpsd=0.0):
        self.is_dead_capital = is_dead_capital
        self.rpsd = rpsd


class FleetEfficiency:
    def __init__(self, channel_efficiencies=None, median_rpsd=0.0):
        self.channel_efficiencies = channel_efficiencies or {}
        self.median_rpsd = median_rpsd


class StubCapitalEfficiency:
    def __init__(self, fleet_efficiency=None, raises=False):
        self.fleet_efficiency = fleet_efficiency
        self.raises = raises

    def analyze(self):
        if self.raises:
            raise RuntimeError("efficiency error")
        return self.fleet_efficiency


DEFAULT_CFG = dict(
    capex_reinvestment_rate=0.50,
    capex_bootstrap_bps=10,
    capex_bootstrap_max_sats=200,
    capex_grace_days=14,
    capex_exploration_rate=0.10,
    capex_tactical_rate=0.15,
    capex_global_envelope_sats=0,
    daily_budget_sats=5000,
    weekly_budget_sats=35000,
    min_wallet_reserve=1_000_000,
    estimated_open_cost_sats=5000,
)


def cfg(**overrides):
    d = dict(DEFAULT_CFG)
    d.update(overrides)
    return types.SimpleNamespace(**d)


def serialize_prof(p):
    return {
        "classification": p.classification,
        "capacity_sats": p.capacity_sats,
        "days_open": p.days_open,
        "marginal_roi": p.marginal_roi,
        "marginal_roi_reliable": p.marginal_roi_reliable,
        "channel_role": p.channel_role,
        "window_30d_available": p.window_30d_available,
        "contribution_30d_msat": p.contribution_30d_msat,
        "fees_earned_30d_msat": p.fees_earned_30d_msat,
        "revenue": {
            "total_contribution_msat": p.revenue.total_contribution_msat,
            "fees_earned_msat": p.revenue.fees_earned_msat,
            "sourced_fee_contribution_msat": p.revenue.sourced_fee_contribution_msat,
            "total_forward_count": p.revenue.total_forward_count,
        },
    }


def serialize_capital_efficiency(ce):
    if ce is None:
        return None
    fe = ce.fleet_efficiency
    if fe is None:
        return None
    return {
        "median_rpsd": fe.median_rpsd,
        "channel_efficiencies": {
            ch: {"is_dead_capital": eff.is_dead_capital, "rpsd": eff.rpsd}
            for ch, eff in fe.channel_efficiencies.items()
        },
    }


def run_scenario(name, *, channels, database_kwargs=None, config_kwargs=None,
                  capital_efficiency=None, bleeders=None):
    database_kwargs = database_kwargs or {}
    config_kwargs = config_kwargs or {}
    db = StubDatabase(**database_kwargs)
    profitability = StubProfitability(db, channels, bleeders=bleeders)
    config = cfg(**config_kwargs)
    engine = CapexBudgetEngine(profitability, db, config, capital_efficiency=capital_efficiency)
    alloc = engine.compute_allocations()

    input_echo = {
        "channels": {ch: serialize_prof(p) for ch, p in channels.items()},
        "capex_by_channel_sats": None if database_kwargs.get("fail_capex") else db.capex_by_channel,
        "spend_summary": None if database_kwargs.get("fail_spend_summary") else db.spend_summary,
        "onchain_sats": db.onchain_sats,
        "success_rates": {
            ch: v["success_rate"] for ch, v in db.success_rates.items()
            if v.get("total", 0) >= 3
        },
        "bleeders": {ch: b.classification for ch, b in (bleeders or {}).items()},
        "fleet_efficiency": serialize_capital_efficiency(capital_efficiency),
        "config": {**DEFAULT_CFG, **config_kwargs},
    }

    out_channel_budgets = {}
    for ch_id, b in alloc.channel_budgets.items():
        out_channel_budgets[ch_id] = {
            "budget_msat": b.budget_msat,
            "budget_sats": b.budget_sats,
            "tier": b.tier,
            "tier_ppm": b.tier_ppm,
            "priority_class": b.priority_class,
            "success_rate_30d": b.success_rate_30d,
            "roi_multiplier": b.roi_multiplier,
        }

    return {
        "name": name,
        "input": input_echo,
        "output": {
            "priority_class": alloc.priority_class,
            "global_envelope_msat": alloc.global_envelope_msat,
            "global_envelope_sats": alloc.global_envelope_sats,
            "fleet_exploration_budget_msat": alloc.fleet_exploration_budget_msat,
            "fleet_exploration_budget_sats": alloc.fleet_exploration_budget_sats,
            "tactical_budget_msat": alloc.tactical_budget_msat,
            "tactical_budget_sats": alloc.tactical_budget_sats,
            "total_fleet_contribution_msat": alloc.total_fleet_contribution_msat,
            "total_fleet_contribution_sats": alloc.total_fleet_contribution_sats,
            "db_degraded": alloc.db_degraded,
            "allocated_by_priority_msat": alloc.allocated_by_priority_msat,
            "allocated_by_priority_sats": alloc.allocated_by_priority_sats,
            "channel_budgets": out_channel_budgets,
        },
    }


scenarios = []

# 1. Proven tier: 30d contribution > 100 sats, positive marginal ROI.
scenarios.append(run_scenario(
    "proven_tier_basic",
    channels={
        "100x1x0": prof(
            classification="profitable", capacity_sats=5_000_000, days_open=100,
            marginal_roi=0.20, marginal_roi_reliable=True,
            window_30d_available=True, contribution_30d_msat=500_000_000,
            fees_earned_30d_msat=500_000_000,
            total_contribution_msat=2_000_000_000, fees_earned_msat=2_000_000_000,
            total_forward_count=50,
        ),
    },
    database_kwargs=dict(capex_by_channel={"100x1x0": 10_000}),
))

# 2. Active tier: forwards > 5 but 30d contribution <= 100 sats.
scenarios.append(run_scenario(
    "active_tier_low_contribution",
    channels={
        "101x1x0": prof(
            classification="break_even", capacity_sats=3_000_000, days_open=40,
            marginal_roi=0.0, marginal_roi_reliable=False,
            window_30d_available=True, contribution_30d_msat=50_000,
            fees_earned_30d_msat=50_000,
            total_contribution_msat=1_000_000, fees_earned_msat=1_000_000,
            total_forward_count=10,
        ),
    },
))

# 3. Bootstrap tier: days_open >= grace, <=5 forwards.
scenarios.append(run_scenario(
    "bootstrap_tier",
    channels={
        "102x1x0": prof(
            classification="break_even", capacity_sats=2_000_000, days_open=20,
            marginal_roi=0.0, total_forward_count=2,
        ),
    },
))

# 4. Blocked: zombie classification.
scenarios.append(run_scenario(
    "blocked_zombie",
    channels={
        "103x1x0": prof(classification="zombie", capacity_sats=1_000_000, days_open=200),
    },
))

# 5. Blocked: hard bleeder bypasses classification.
scenarios.append(run_scenario(
    "blocked_hard_bleeder",
    channels={
        "104x1x0": prof(classification="underwater", capacity_sats=1_000_000, days_open=200),
    },
    bleeders={"104x1x0": StubBleeder("hard")},
))

# 6. Blocked: within grace period, zero contribution.
scenarios.append(run_scenario(
    "blocked_grace_period",
    channels={
        "105x1x0": prof(classification="break_even", capacity_sats=1_000_000, days_open=3,
                          total_contribution_msat=0),
    },
))

# 7. ROI multiplier clamping: very high marginal ROI clamps to 1.5x.
scenarios.append(run_scenario(
    "roi_multiplier_clamp_high",
    channels={
        "106x1x0": prof(
            classification="profitable", capacity_sats=5_000_000, days_open=100,
            marginal_roi=5.0, marginal_roi_reliable=True,
            window_30d_available=True, contribution_30d_msat=500_000_000,
            fees_earned_30d_msat=500_000_000, total_forward_count=50,
        ),
    },
))

# 8. ROI multiplier clamping: very negative marginal ROI clamps to 0.25x.
scenarios.append(run_scenario(
    "roi_multiplier_clamp_low",
    channels={
        "107x1x0": prof(
            classification="profitable", capacity_sats=5_000_000, days_open=100,
            marginal_roi=-5.0, marginal_roi_reliable=True,
            window_30d_available=True, contribution_30d_msat=500_000_000,
            fees_earned_30d_msat=500_000_000, total_forward_count=50,
        ),
    },
))

# 9. ROI unreliable -> neutral multiplier 1.0.
scenarios.append(run_scenario(
    "roi_unreliable_neutral",
    channels={
        "108x1x0": prof(
            classification="profitable", capacity_sats=5_000_000, days_open=100,
            marginal_roi=1.0, marginal_roi_reliable=False,
            window_30d_available=True, contribution_30d_msat=500_000_000,
            fees_earned_30d_msat=500_000_000, total_forward_count=50,
        ),
    },
))

# 10. db_degraded: capex read fails -> fail closed, all budgets zeroed.
scenarios.append(run_scenario(
    "db_degraded_capex_read_fails",
    channels={
        "109x1x0": prof(
            classification="profitable", capacity_sats=5_000_000, days_open=100,
            marginal_roi=0.20, window_30d_available=True,
            contribution_30d_msat=500_000_000, fees_earned_30d_msat=500_000_000,
            total_forward_count=50,
        ),
    },
    database_kwargs=dict(fail_capex=True),
))

# 11. db_degraded: spend summary read fails.
scenarios.append(run_scenario(
    "db_degraded_spend_summary_fails",
    channels={
        "110x1x0": prof(
            classification="profitable", capacity_sats=5_000_000, days_open=100,
            marginal_roi=0.20, window_30d_available=True,
            contribution_30d_msat=500_000_000, fees_earned_30d_msat=500_000_000,
            total_forward_count=50,
        ),
    },
    database_kwargs=dict(fail_spend_summary=True),
))

# 12. Global envelope scale-down: raw total exceeds configured envelope.
scenarios.append(run_scenario(
    "envelope_scale_down",
    channels={
        "111x1x0": prof(
            classification="profitable", capacity_sats=50_000_000, days_open=100,
            marginal_roi=0.20, window_30d_available=True,
            contribution_30d_msat=50_000_000_000, fees_earned_30d_msat=50_000_000_000,
            total_forward_count=50,
        ),
    },
    config_kwargs=dict(capex_global_envelope_sats=1000),
))

# 13. Priority class: defensive (multiple hard bleeders).
scenarios.append(run_scenario(
    "priority_class_defensive_multi_bleeder",
    channels={
        "112x1x0": prof(classification="underwater", capacity_sats=1_000_000, days_open=200),
        "113x1x0": prof(classification="underwater", capacity_sats=1_000_000, days_open=200),
    },
    bleeders={
        "112x1x0": StubBleeder("hard"),
        "113x1x0": StubBleeder("hard"),
    },
))

# 14. Priority class: defensive via capacity threshold (single big bleeder >10% capacity).
scenarios.append(run_scenario(
    "priority_class_defensive_capacity_threshold",
    channels={
        "114x1x0": prof(classification="underwater", capacity_sats=9_000_000, days_open=200),
        "115x1x0": prof(classification="break_even", capacity_sats=1_000_000, days_open=200,
                          total_forward_count=10),
    },
    bleeders={"114x1x0": StubBleeder("hard")},
))

# 15. Priority class: preservation (depleted earner present, no hard bleeders).
scenarios.append(run_scenario(
    "priority_class_preservation",
    channels={
        "116x1x0": prof(classification="underwater", capacity_sats=1_000_000, days_open=200,
                          total_contribution_msat=200_000),
    },
))

# 16. Priority class: operational (reserve deficit, no bleeders/depleted).
scenarios.append(run_scenario(
    "priority_class_operational",
    channels={
        "117x1x0": prof(classification="break_even", capacity_sats=1_000_000, days_open=200,
                          total_forward_count=10),
    },
    database_kwargs=dict(onchain_sats=500_000),
    config_kwargs=dict(min_wallet_reserve=1_000_000),
))

# 17. Priority class: growth (no bleeders, no depleted, no reserve deficit).
scenarios.append(run_scenario(
    "priority_class_growth",
    channels={
        "118x1x0": prof(classification="break_even", capacity_sats=1_000_000, days_open=200,
                          total_forward_count=10),
    },
    database_kwargs=dict(onchain_sats=5_000_000),
))

# 18. Efficiency multiplier: dead capital zeroes budget (no gateway value).
scenarios.append(run_scenario(
    "efficiency_dead_capital_zero",
    channels={
        "119x1x0": prof(
            classification="profitable", capacity_sats=5_000_000, days_open=100,
            marginal_roi=0.20, window_30d_available=True,
            contribution_30d_msat=500_000_000, fees_earned_30d_msat=500_000_000,
            total_forward_count=50,
        ),
    },
    capital_efficiency=StubCapitalEfficiency(FleetEfficiency(
        channel_efficiencies={"119x1x0": ChannelEff(is_dead_capital=True)},
        median_rpsd=10.0,
    )),
))

# 19. Efficiency multiplier: dead capital with gateway value floors at 0.25.
scenarios.append(run_scenario(
    "efficiency_dead_capital_gateway_floor",
    channels={
        "120x1x0": prof(
            classification="profitable", capacity_sats=5_000_000, days_open=100,
            marginal_roi=0.20, window_30d_available=True,
            contribution_30d_msat=500_000_000, fees_earned_30d_msat=500_000_000,
            total_forward_count=50, channel_role="inbound_gateway",
        ),
    },
    capital_efficiency=StubCapitalEfficiency(FleetEfficiency(
        channel_efficiencies={"120x1x0": ChannelEff(is_dead_capital=True)},
        median_rpsd=10.0,
    )),
))

# 20. Efficiency multiplier: below-median rpsd scales down (floored at 0.5).
scenarios.append(run_scenario(
    "efficiency_below_median_rpsd",
    channels={
        "121x1x0": prof(
            classification="profitable", capacity_sats=5_000_000, days_open=100,
            marginal_roi=0.20, window_30d_available=True,
            contribution_30d_msat=500_000_000, fees_earned_30d_msat=500_000_000,
            total_forward_count=50,
        ),
    },
    capital_efficiency=StubCapitalEfficiency(FleetEfficiency(
        channel_efficiencies={"121x1x0": ChannelEff(is_dead_capital=False, rpsd=1.0)},
        median_rpsd=10.0,
    )),
))

# 21. Efficiency multiplier: above-median rpsd scales up (capped at 1.5).
scenarios.append(run_scenario(
    "efficiency_above_median_rpsd",
    channels={
        "122x1x0": prof(
            classification="profitable", capacity_sats=5_000_000, days_open=100,
            marginal_roi=0.20, window_30d_available=True,
            contribution_30d_msat=500_000_000, fees_earned_30d_msat=500_000_000,
            total_forward_count=50,
        ),
    },
    capital_efficiency=StubCapitalEfficiency(FleetEfficiency(
        channel_efficiencies={"122x1x0": ChannelEff(is_dead_capital=False, rpsd=100.0)},
        median_rpsd=10.0,
    )),
))

# 22. No window data: falls back to lifetime contribution for funding.
scenarios.append(run_scenario(
    "no_window_data_lifetime_fallback",
    channels={
        "123x1x0": prof(
            classification="profitable", capacity_sats=5_000_000, days_open=100,
            marginal_roi=0.20, window_30d_available=False,
            total_contribution_msat=500_000_000, fees_earned_msat=500_000_000,
            total_forward_count=50,
        ),
    },
))

# 23. Empty fleet (no channels at all): growth priority, zero everything.
scenarios.append(run_scenario("empty_fleet", channels={}))

# 24. Spend depletion: exploration budget reduced by prior category spend.
scenarios.append(run_scenario(
    "spend_depletion_exploration",
    channels={
        "124x1x0": prof(
            classification="profitable", capacity_sats=5_000_000, days_open=100,
            marginal_roi=0.20, window_30d_available=True,
            contribution_30d_msat=500_000_000, fees_earned_30d_msat=500_000_000,
            total_forward_count=50,
        ),
    },
    database_kwargs=dict(spend_summary={
        "spent_by_category": {"channel_open": 1000},
        "reserved_by_category": {"channel_open": 500},
    }),
))

# --- attribute_boltz_cost / record_boltz_spend / reserve-settle-release ---

def run_boltz_lifecycle():
    db = StubDatabase()
    profitability = StubProfitability(db, {})
    config = cfg()
    engine = CapexBudgetEngine(profitability, db, config)

    split_channel = engine.attribute_boltz_cost(1001, channel_id="200x1x0")
    split_treasury = engine.attribute_boltz_cost(1000, channel_id=None)

    reserve_ok = engine.reserve_boltz_swap_budget(
        "res-1", 5000, channel_id="200x1x0", effective_budget_sats=10000)
    settle_ok = engine.settle_boltz_swap_reservation(
        "res-1", "swap-1", 5000, channel_id="200x1x0")
    settle_reservation_released = "res-1" not in db.reservations
    spend_event = db.spend_events[-1] if db.spend_events else None

    reserve_ok2 = engine.reserve_boltz_swap_budget(
        "res-2", 3000, channel_id=None, effective_budget_sats=10000)
    release_ok2 = engine.release_boltz_swap_reservation("res-2")
    reservation2_released = "res-2" not in db.reservations

    record_zero_fee_rejected = engine.record_boltz_spend("swap-2", 0)
    record_negative_swap_id_rejected = engine.record_boltz_spend("", 500)

    return {
        "name": "boltz_lifecycle",
        "output": {
            "split_channel": split_channel,
            "split_treasury": split_treasury,
            "reserve_ok": reserve_ok,
            "settle_ok": settle_ok,
            "settle_reservation_released": settle_reservation_released,
            "spend_event": {
                "event_id": spend_event["event_id"],
                "category": spend_event["category"],
                "amount_sats": spend_event["amount_sats"],
                "subcategory": spend_event["subcategory"],
                "channel_id": spend_event["channel_id"],
            } if spend_event else None,
            "reserve_ok2": reserve_ok2,
            "release_ok2": release_ok2,
            "reservation2_released": reservation2_released,
            "record_zero_fee_rejected": record_zero_fee_rejected,
            "record_negative_swap_id_rejected": record_negative_swap_id_rejected,
        },
    }


scenarios.append(run_boltz_lifecycle())


def run_settle_write_failure():
    class FailingDB(StubDatabase):
        def record_spend_event(self, **kwargs):
            return False

    db = FailingDB()
    profitability = StubProfitability(db, {})
    config = cfg()
    engine = CapexBudgetEngine(profitability, db, config)
    engine.reserve_boltz_swap_budget("res-3", 4000, effective_budget_sats=10000)
    settle_ok = engine.settle_boltz_swap_reservation("res-3", "swap-3", 4000)
    reservation_still_active = "res-3" in db.reservations
    return {
        "name": "settle_write_failure_keeps_reservation",
        "output": {
            "settle_ok": settle_ok,
            "reservation_still_active": reservation_still_active,
        },
    }


scenarios.append(run_settle_write_failure())

out = {"scenarios": scenarios}
print(json.dumps(out, indent=2, sort_keys=True))
