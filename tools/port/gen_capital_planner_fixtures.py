#!/usr/bin/env python3
"""Generate real-Python fixtures for the CapacityPlanner pure-kernel subset
ported into revops-capital.

Runs the ACTUAL modules/capacity_planner.py (imported unmodified from
/home/sat/bin/cl_revenue_ops) against constructed scenarios for the
functions that are pure decisions given already-fetched evidence:
  - _check_portfolio_balance_gate
  - _close_fee_reserve_multiplier / _configured_close_fee_cap_sats /
    _close_fee_plan / _close_feerange / _extract_actual_close_fee_sats
  - _extract_close_feerate_perkb / _extract_opening_feerate_perkb
  - _failed_open_backoff_reason (given a pre-fetched action list)
  - _peer_exposure_cap_reason (given a pre-fetched channel list)
  - _calculate_open_ev / _calculate_redeployment_ev / _calculate_recycle_ev /
    _is_recycle_eligible
  - _normalize_candidate_scores / _apply_pool_quotas
  - the dead-capital stage machine inside _build_dead_capital_loser (with
    _close_protection_reason and _dead_capital_defib_attempted monkeypatched
    to injected values, isolating the pure state-transition logic)
"""
import json
import sys
import time
import types

sys.path.insert(0, "/home/sat/bin/cl_revenue_ops")

from modules.capacity_planner import CapacityPlanner  # noqa: E402
from modules.profitability_analyzer import ProfitabilityClass, ChannelRole  # noqa: E402
from modules.demand_flow import DemandFlowClassifier  # noqa: E402


class StubPlugin:
    def log(self, *a, **k):
        pass

    rpc = None


class StubDatabase:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.recent_actions = kwargs.get("recent_actions", [])
        self.reservations = {}

    def get_recent_planner_actions(self, peer_id, hours=24):
        return self.recent_actions

    def get_peer_closed_channel_profit_summary(self, peer_id):
        return getattr(self, "closed_summary", None)

    def get_historical_inbound_fee_ppm(self, peer_id):
        return getattr(self, "inbound_fee_data", None)

    def get_diagnostic_rebalance_stats(self, scid, days=14):
        return getattr(self, "diag_stats", {"attempt_count": 0})

    def get_peer_uptime_percent(self, peer_id, duration_seconds=None):
        return getattr(self, "uptime_pct", None)

    def upsert_dead_capital_stage(self, scid, stage, entered_at):
        self.upserted = (scid, stage, entered_at)

    def get_channel_rebalance_success_rate(self, ch_id, days):
        return getattr(self, "success_data", None)

    def record_planner_action(self, **kwargs):
        return 1

    def update_planner_action(self, action_id, status):
        pass

    def get_fee_strategy_state(self, scid):
        return getattr(self, "fee_strategy_state", None)

    def get_top_route_pairs(self, days=30, min_forwards=3, limit=10):
        return getattr(self, "route_pairs", [])

    def get_peer_reputation(self, peer_id):
        return getattr(self, "peer_reputation", None)

    def get_peer_closed_channel_profit_summary(self, peer_id):
        # Overridden per-instance below for score_candidate cases; keep the
        # winners/ev-case attribute name (`closed_summary`) as the primary
        # source so existing scenarios above are unaffected.
        return getattr(self, "closed_summary", None)


class StubProfitability:
    def __init__(self, database, bleeders=None):
        self.database = database
        self._bleeders = bleeders or []

    def identify_bleeders_v2(self):
        return self._bleeders


def make_planner(db=None, data_service=None, bleeders=None):
    db = db or StubDatabase()
    profitability = StubProfitability(db, bleeders=bleeders)
    flow = types.SimpleNamespace()
    cp = CapacityPlanner(StubPlugin(), profitability, flow)
    cp.data_service = data_service
    return cp


def cfg(**overrides):
    d = dict(
        planner_close_fee_reserve_multiplier=2.0,
        planner_close_fee_cap_sats=0,
        planner_close_feerange_enabled=False,
        planner_max_channel_sats=10_000_000,
        planner_max_fee_rate_sat_vb=50.0,
        planner_min_annual_roi_pct=1.0,
    )
    d.update(overrides)
    return types.SimpleNamespace(**d)


scenarios = []


# --- portfolio balance gate --------------------------------------------
def portfolio_gate_case(name, channels):
    cp = make_planner()
    return {"name": name, "kind": "portfolio_gate",
            "input": {"channels": channels},
            "output": cp._check_portfolio_balance_gate(channels)}


scenarios.append(portfolio_gate_case("healthy_no_channels", []))
scenarios.append(portfolio_gate_case("healthy_60pct", [
    {"state": "CHANNELD_NORMAL", "to_us_msat": 600_000_000, "total_msat": 1_000_000_000},
]))
scenarios.append(portfolio_gate_case("watch_75pct", [
    {"state": "CHANNELD_NORMAL", "to_us_msat": 750_000_000, "total_msat": 1_000_000_000},
]))
scenarios.append(portfolio_gate_case("constrained_85pct", [
    {"state": "CHANNELD_NORMAL", "to_us_msat": 850_000_000, "total_msat": 1_000_000_000},
]))
scenarios.append(portfolio_gate_case("blocked_96pct", [
    {"state": "CHANNELD_NORMAL", "to_us_msat": 960_000_000, "total_msat": 1_000_000_000},
]))
scenarios.append(portfolio_gate_case("blocked_boundary_95pct_exact", [
    {"state": "CHANNELD_NORMAL", "to_us_msat": 950_000_000, "total_msat": 1_000_000_000},
]))
scenarios.append(portfolio_gate_case("non_normal_channels_ignored", [
    {"state": "CHANNELD_AWAITING_LOCKIN", "to_us_msat": 999_000_000, "total_msat": 1_000_000_000},
]))
scenarios.append(portfolio_gate_case("mixed_channels", [
    {"state": "CHANNELD_NORMAL", "to_us_msat": 900_000_000, "total_msat": 1_000_000_000},
    {"state": "CHANNELD_NORMAL", "to_us_msat": 100_000_000, "total_msat": 1_000_000_000},
]))


# --- close fee plan / multiplier / cap / feerange -----------------------
def close_fee_case(name, *, feerates, cfg_kwargs):
    def get_feerates(style):
        return feerates

    ds = types.SimpleNamespace(get_feerates=get_feerates)
    cp = make_planner(data_service=ds)
    c = cfg(**cfg_kwargs)
    plan = cp._close_fee_plan(c)
    return {"name": name, "kind": "close_fee_plan",
            "input": {"feerates": feerates, "cfg": cfg_kwargs},
            "output": plan}


scenarios.append(close_fee_case(
    "multiplier_default",
    feerates={"perkb": {"mutual_close": 2000}},
    cfg_kwargs={},
))
scenarios.append(close_fee_case(
    "multiplier_custom_2_5x",
    feerates={"perkb": {"mutual_close": 4000}},
    cfg_kwargs={"planner_close_fee_reserve_multiplier": 2.5},
))
scenarios.append(close_fee_case(
    "fixed_cap_sufficient",
    feerates={"perkb": {"mutual_close": 1000}},
    cfg_kwargs={"planner_close_fee_cap_sats": 1000},
))
scenarios.append(close_fee_case(
    "fixed_cap_insufficient",
    feerates={"perkb": {"mutual_close": 100000}},
    cfg_kwargs={"planner_close_fee_cap_sats": 100},
))
scenarios.append(close_fee_case(
    "feerange_enabled",
    feerates={"perkb": {"mutual_close": 4000}},
    cfg_kwargs={"planner_close_feerange_enabled": True},
))
scenarios.append(close_fee_case(
    "feerate_fallback_unilateral",
    feerates={"perkb": {"unilateral_close": 3000}},
    cfg_kwargs={},
))
scenarios.append(close_fee_case(
    "feerate_fallback_opening",
    feerates={"perkb": {"opening": 5000}},
    cfg_kwargs={},
))
scenarios.append(close_fee_case(
    "feerate_missing_uses_chaincost_default",
    feerates={},
    cfg_kwargs={},
))

for mult, name in [(1.0, "multiplier_clamped_to_1_0_floor"), (0.1, "multiplier_below_1_clamped")]:
    scenarios.append(close_fee_case(
        name,
        feerates={"perkb": {"mutual_close": 2000}},
        cfg_kwargs={"planner_close_fee_reserve_multiplier": mult},
    ))


# --- extract_actual_close_fee_sats --------------------------------------
def extract_close_fee_case(name, result):
    cp = make_planner()
    return {"name": name, "kind": "extract_actual_close_fee_sats",
            "input": {"result": result},
            "output": cp._extract_actual_close_fee_sats(result)}


scenarios.append(extract_close_fee_case("int_field", {"actual_fee_sats": 1500}))
scenarios.append(extract_close_fee_case("string_field_with_sat_suffix", {"close_fee_sats": "1200sat"}))
scenarios.append(extract_close_fee_case("msat_field", {"fee_msat": 1500500}))
scenarios.append(extract_close_fee_case("bool_field_skipped", {"actual_fee_sats": True, "fee_sats": 900}))
scenarios.append(extract_close_fee_case("no_matching_fields", {"foo": "bar"}))
scenarios.append(extract_close_fee_case("not_a_dict", "not-a-dict"))


# --- failed_open_backoff_reason ------------------------------------------
def backoff_case(name, actions, *, now_offset_from_last_failure_hours=None):
    db = StubDatabase(recent_actions=actions)
    cp = make_planner(db=db)
    reason = cp._failed_open_backoff_reason("peer_abcdefabcdefabcdef")
    return {"name": name, "kind": "failed_open_backoff_reason",
            "input": {"actions": actions, "now": now},
            "output_is_none": reason is None,
            "output": reason}


now = int(time.time())
scenarios.append(backoff_case("no_actions", []))
scenarios.append(backoff_case("single_recent_failure_blocks", [
    {"action_type": "open", "status": "failed", "created_at": now - 1800},
]))
scenarios.append(backoff_case("single_old_failure_clears", [
    {"action_type": "open", "status": "failed", "created_at": now - 3 * 3600},
]))
scenarios.append(backoff_case("success_resets_streak", [
    {"action_type": "open", "status": "failed", "created_at": now - 100},
    {"action_type": "open", "status": "completed", "created_at": now - 200},
]))
scenarios.append(backoff_case("dry_run_resets_streak", [
    {"action_type": "open", "status": "failed", "created_at": now - 100},
    {"action_type": "open", "status": "dry_run", "created_at": now - 200},
]))
scenarios.append(backoff_case("non_open_actions_ignored", [
    {"action_type": "close", "status": "failed", "created_at": now - 100},
]))
scenarios.append(backoff_case("many_failures_capped_at_168h", [
    {"action_type": "open", "status": "failed", "created_at": now - 3600 * i}
    for i in range(1, 12)
]))


# --- peer_exposure_cap_reason ---------------------------------------------
def exposure_case(name, channels, *, max_channel_sats=1_000_000):
    cp = make_planner()
    cp._cycle_peer_channels = channels
    c = cfg(planner_max_channel_sats=max_channel_sats)
    reason = cp._peer_exposure_cap_reason("peer_abcdefabcdefabcdef", c)
    return {"name": name, "kind": "peer_exposure_cap_reason",
            "input": {"channels": channels, "max_channel_sats": max_channel_sats},
            "output_is_none": reason is None,
            "output": reason}


scenarios.append(exposure_case("under_cap", [
    {"peer_id": "peer_abcdefabcdefabcdef", "state": "CHANNELD_NORMAL", "total_msat": 500_000_000},
]))
scenarios.append(exposure_case("at_cap_blocks", [
    {"peer_id": "peer_abcdefabcdefabcdef", "state": "CHANNELD_NORMAL", "total_msat": 2_000_000_000},
]))
scenarios.append(exposure_case("over_cap_blocks", [
    {"peer_id": "peer_abcdefabcdefabcdef", "state": "CHANNELD_NORMAL", "total_msat": 3_000_000_000},
]))
scenarios.append(exposure_case("other_peer_ignored", [
    {"peer_id": "someone_else", "state": "CHANNELD_NORMAL", "total_msat": 5_000_000_000},
]))
scenarios.append(exposure_case("non_normal_state_ignored", [
    {"peer_id": "peer_abcdefabcdefabcdef", "state": "CHANNELD_AWAITING_LOCKIN", "total_msat": 5_000_000_000},
]))
scenarios.append(exposure_case("cap_disabled_zero", [
    {"peer_id": "peer_abcdefabcdefabcdef", "state": "CHANNELD_NORMAL", "total_msat": 5_000_000_000},
], max_channel_sats=0))


# --- calculate_open_ev ----------------------------------------------------
def open_ev_case(name, *, closed_summary=None, inbound_fee_data=None,
                 observed_ppm_cache=None, channel_size_sats=1_000_000,
                 min_annual_roi_pct=1.0, feerates=None):
    db = StubDatabase(closed_summary=closed_summary, inbound_fee_data=inbound_fee_data)
    def get_feerates(style):
        return feerates or {"perkb": {"opening": 1000, "mutual_close": 1000}}
    ds = types.SimpleNamespace(get_feerates=get_feerates)
    cp = make_planner(db=db, data_service=ds)
    cp._observed_daily_ppm_cache = (observed_ppm_cache,)
    c = cfg(planner_min_annual_roi_pct=min_annual_roi_pct)
    ev = cp._calculate_open_ev("peer1", channel_size_sats, c)
    return {"name": name, "kind": "calculate_open_ev",
            "input": {"closed_summary": closed_summary, "inbound_fee_data": inbound_fee_data,
                       "observed_ppm_cache": observed_ppm_cache,
                       "channel_size_sats": channel_size_sats,
                       "min_annual_roi_pct": min_annual_roi_pct, "feerates": feerates},
            "output": ev}


scenarios.append(open_ev_case("bootstrap_no_history", observed_ppm_cache=None))
scenarios.append(open_ev_case("observed_ppm_anchor", observed_ppm_cache=20.0))
scenarios.append(open_ev_case("observed_ppm_ceiling_clamped", observed_ppm_cache=1000.0))
scenarios.append(open_ev_case(
    "closed_channel_profit_inheritance",
    closed_summary={"daily_net_est_sats": 50.0},
    observed_ppm_cache=20.0,
))
scenarios.append(open_ev_case(
    "inbound_fee_history_used",
    inbound_fee_data={"median_fee_ppm": 300},
    observed_ppm_cache=20.0,
))
scenarios.append(open_ev_case(
    "large_channel_size",
    observed_ppm_cache=20.0,
    channel_size_sats=20_000_000,
))
scenarios.append(open_ev_case(
    "high_roi_hurdle",
    observed_ppm_cache=20.0,
    min_annual_roi_pct=50.0,
))
scenarios.append(open_ev_case(
    "custom_feerates",
    observed_ppm_cache=20.0,
    feerates={"perkb": {"opening": 5000, "mutual_close": 3000}},
))


# --- calculate_redeployment_ev / recycle_ev / is_recycle_eligible --------
def redeployment_ev_case(name, loser, winners, *, observed_ppm_cache=20.0, feerates=None):
    db = StubDatabase()
    def get_feerates(style):
        return feerates or {"perkb": {"opening": 1000, "mutual_close": 1000}}
    ds = types.SimpleNamespace(get_feerates=get_feerates)
    cp = make_planner(db=db, data_service=ds)
    cp._observed_daily_ppm_cache = (observed_ppm_cache,)
    c = cfg()
    redeployment_ev, best_peer, best_ev = cp._calculate_redeployment_ev(loser, winners, c)
    return {"name": name, "kind": "calculate_redeployment_ev",
            "input": {"loser": loser, "winners": winners, "observed_ppm_cache": observed_ppm_cache},
            "output": {"redeployment_ev": redeployment_ev, "best_peer": best_peer, "best_ev": best_ev}}


scenarios.append(redeployment_ev_case(
    "no_winners",
    {"capacity": 1_000_000, "marginal_profit_30d_sats": -500},
    [],
))
scenarios.append(redeployment_ev_case(
    "one_winner_positive_redeployment",
    {"capacity": 1_000_000, "marginal_profit_30d_sats": -1000},
    [{"peer_id": "winner1"}],
))
scenarios.append(redeployment_ev_case(
    "bleeding_loser_negative_residual_favors_close",
    {"capacity": 1_000_000, "marginal_profit_30d_sats": -5000},
    [{"peer_id": "winner1"}],
))
scenarios.append(redeployment_ev_case(
    "multiple_winners_picks_best",
    {"capacity": 1_000_000, "marginal_profit_30d_sats": 0},
    [{"peer_id": "winner_low"}, {"peer_id": "winner_high"}],
    observed_ppm_cache=20.0,
))


def recycle_ev_case(name, loser, candidate, *, observed_ppm_cache=20.0):
    db = StubDatabase()
    def get_feerates(style):
        return {"perkb": {"opening": 1000, "mutual_close": 1000}}
    ds = types.SimpleNamespace(get_feerates=get_feerates)
    cp = make_planner(db=db, data_service=ds)
    cp._observed_daily_ppm_cache = (observed_ppm_cache,)
    c = cfg()
    ev = cp._calculate_recycle_ev(loser, candidate, c)
    return {"name": name, "kind": "calculate_recycle_ev",
            "input": {"loser": loser, "candidate": candidate, "observed_ppm_cache": observed_ppm_cache},
            "output": ev}


scenarios.append(recycle_ev_case(
    "basic_recycle",
    {"capacity": 1_000_000, "marginal_profit_30d_sats": -1000},
    {"peer_id": "candidate1"},
))
scenarios.append(recycle_ev_case(
    "positive_residual_reduces_ev",
    {"capacity": 1_000_000, "marginal_profit_30d_sats": 500},
    {"peer_id": "candidate1"},
))


def recycle_eligible_case(name, loser, protected_peers, route_pair_scids, *, block_height=800_000):
    ds = types.SimpleNamespace(get_block_height=lambda: block_height)
    cp = make_planner(data_service=ds)
    ok, reason = cp._is_recycle_eligible(loser, protected_peers, route_pair_scids)
    return {"name": name, "kind": "is_recycle_eligible",
            "input": {"loser": loser, "protected_peers": list(protected_peers) if protected_peers is not None else None,
                       "route_pair_scids": list(route_pair_scids), "block_height": block_height},
            "output": {"ok": ok, "reason": reason}}


scenarios.append(recycle_eligible_case(
    "eligible",
    {"scid": "700000x1x0", "peer_id": "peerA", "marginal_roi": -5.0},
    set(), set(),
    block_height=800_000,  # (800000-700000)*10/1440 = ~694 days
))
scenarios.append(recycle_eligible_case(
    "too_young",
    {"scid": "799999x1x0", "peer_id": "peerA", "marginal_roi": -5.0},
    set(), set(),
    block_height=800_000,
))
scenarios.append(recycle_eligible_case(
    "positive_roi_ineligible",
    {"scid": "700000x1x0", "peer_id": "peerA", "marginal_roi": 5.0},
    set(), set(),
))
scenarios.append(recycle_eligible_case(
    "policy_protected",
    {"scid": "700000x1x0", "peer_id": "peerA", "marginal_roi": -5.0},
    {"peerA"}, set(),
))
scenarios.append(recycle_eligible_case(
    "policy_unknown_fails_closed",
    {"scid": "700000x1x0", "peer_id": "peerA", "marginal_roi": -5.0},
    None, set(),
))
scenarios.append(recycle_eligible_case(
    "route_pair_protected",
    {"scid": "700000x1x0", "peer_id": "peerA", "marginal_roi": -5.0},
    set(), {"700000x1x0"},
))


# --- normalize_candidate_scores / apply_pool_quotas -----------------------
def normalize_case(name, candidates):
    cp = make_planner()
    out = cp._normalize_candidate_scores([dict(c) for c in candidates])
    return {"name": name, "kind": "normalize_candidate_scores",
            "input": {"candidates": candidates}, "output": out}


scenarios.append(normalize_case("winner_at_floor", [
    {"source": "winner", "score": 0.20, "peer_id": "p1"},
]))
scenarios.append(normalize_case("winner_below_floor_dropped", [
    {"source": "winner", "score": 0.10, "peer_id": "p1"},
]))
scenarios.append(normalize_case("winner_clamped_at_2x_anchor", [
    {"source": "winner", "score": 5.0, "peer_id": "p1"},
]))
scenarios.append(normalize_case("graph_scaled_by_50x_anchor", [
    {"source": "graph", "score": 25.0, "peer_id": "p1"},
]))
scenarios.append(normalize_case("route_pair_scale", [
    {"source": "route_pair", "score": 0.2, "peer_id": "p1"},
]))
scenarios.append(normalize_case("demand_flow_scale", [
    {"source": "demand_flow", "score": 0.4, "peer_id": "p1"},
]))
scenarios.append(normalize_case("unknown_source_defaults", [
    {"source": "mystery", "score": 0.5, "peer_id": "p1"},
]))
scenarios.append(normalize_case("nan_score_treated_as_zero", [
    {"source": "winner", "score": "NaN", "peer_id": "p1"},
]))
scenarios.append(normalize_case("mixed_group", [
    {"source": "winner", "score": 0.45, "peer_id": "p1"},
    {"source": "neighbor", "score": 0.8, "peer_id": "p2"},
    {"source": "graph", "score": 10.0, "peer_id": "p3"},
]))


def quota_case(name, candidates, max_pool=32):
    cp = make_planner()
    out = cp._apply_pool_quotas([dict(c) for c in candidates], max_pool=max_pool)
    return {"name": name, "kind": "apply_pool_quotas",
            "input": {"candidates": candidates, "max_pool": max_pool},
            "output": [c["peer_id"] for c in out]}


scenarios.append(quota_case("reserved_quota_respected", [
    {"source": "graph", "score": 1.0, "peer_id": f"g{i}"} for i in range(6)
] + [
    {"source": "route_pair", "score": 1.0, "peer_id": f"r{i}"} for i in range(6)
] + [
    {"source": "winner", "score": 0.9, "peer_id": f"w{i}"} for i in range(30)
]))
scenarios.append(quota_case("small_pool_no_reserved_candidates", [
    {"source": "winner", "score": 0.9, "peer_id": "w1"},
    {"source": "neighbor", "score": 0.8, "peer_id": "n1"},
]))
unfilled_slot_candidates = [{"source": "graph", "score": 1.0, "peer_id": "g1"}] + [
    {"source": "winner", "score": 0.9 - i * 0.01, "peer_id": f"w{i}"} for i in range(40)
]
scenarios.append(quota_case(
    "unfilled_reserved_slots_go_to_open_pool", unfilled_slot_candidates, max_pool=10))
scenarios.append(quota_case("max_pool_cap_enforced", [
    {"source": "winner", "score": 1.0 - i * 0.001, "peer_id": f"w{i}"} for i in range(50)
], max_pool=32))


# --- dead-capital stage machine (monkeypatched pure isolation) -----------
def dead_capital_case(name, *, stage_row, opener, close_protection, defib_attempted,
                       is_dead_capital=True, now_offset=0):
    db = StubDatabase()
    cp = make_planner(db=db)

    fixed_now = int(time.time()) + now_offset
    import modules.capacity_planner as cap_mod
    real_time = cap_mod.time.time
    cap_mod.time.time = lambda: fixed_now
    try:
        cp._close_protection_reason = lambda *a, **k: close_protection
        cp._dead_capital_defib_attempted = lambda scid: defib_attempted
        prof = types.SimpleNamespace(
            opener=opener, classification="underwater", roi_percent=-10.0,
            marginal_roi_percent=-20.0, capacity_sats=1_000_000, peer_id="peerX",
            marginal_profit_30d_sats=-100,
        )
        channel_efficiency = types.SimpleNamespace(
            is_dead_capital=is_dead_capital, dead_capital_stage=stage_row.get("stage", "none"))
        dead_capital_stages = {"999x1x0": stage_row} if stage_row else {}
        result = cp._build_dead_capital_loser(
            "999x1x0", prof, channel_efficiency, dead_capital_stages)
    finally:
        cap_mod.time.time = real_time

    return {"name": name, "kind": "dead_capital_stage",
            "input": {"stage_row": stage_row, "opener": opener,
                       "close_protection": close_protection, "defib_attempted": defib_attempted,
                       "now": fixed_now},
            "output": {"action": result["action"], "stage": result["dead_capital_stage"],
                       "close_protection_out": result["close_protection"]} if result else None}


now_i = int(time.time())
scenarios.append(dead_capital_case(
    "none_to_fee_reduction", stage_row={}, opener="local",
    close_protection=None, defib_attempted=False))
scenarios.append(dead_capital_case(
    "fee_reduction_before_timeout_stays", stage_row={"stage": "fee_reduction", "entered_at": now_i - 3600},
    opener="local", close_protection=None, defib_attempted=False))
scenarios.append(dead_capital_case(
    "fee_reduction_local_timeout_advances", stage_row={"stage": "fee_reduction", "entered_at": now_i - 25 * 3600},
    opener="local", close_protection=None, defib_attempted=False))
scenarios.append(dead_capital_case(
    "fee_reduction_remote_needs_48h", stage_row={"stage": "fee_reduction", "entered_at": now_i - 25 * 3600},
    opener="remote", close_protection=None, defib_attempted=False))
scenarios.append(dead_capital_case(
    "fee_reduction_remote_48h_advances", stage_row={"stage": "fee_reduction", "entered_at": now_i - 49 * 3600},
    opener="remote", close_protection=None, defib_attempted=False))
scenarios.append(dead_capital_case(
    "defibrillation_before_timeout_stays", stage_row={"stage": "defibrillation", "entered_at": now_i - 3600},
    opener="local", close_protection=None, defib_attempted=True))
scenarios.append(dead_capital_case(
    "defibrillation_timeout_no_protection_attempted_closes",
    stage_row={"stage": "defibrillation", "entered_at": now_i - 25 * 3600},
    opener="local", close_protection=None, defib_attempted=True))
scenarios.append(dead_capital_case(
    "defibrillation_timeout_protected_holds",
    stage_row={"stage": "defibrillation", "entered_at": now_i - 25 * 3600},
    opener="local", close_protection="inbound_gateway_protected", defib_attempted=True))
scenarios.append(dead_capital_case(
    "defibrillation_timeout_no_attempt_holds",
    stage_row={"stage": "defibrillation", "entered_at": now_i - 25 * 3600},
    opener="local", close_protection=None, defib_attempted=False))
scenarios.append(dead_capital_case(
    "close_stays_close", stage_row={"stage": "close", "entered_at": now_i - 100},
    opener="local", close_protection=None, defib_attempted=True))
scenarios.append(dead_capital_case(
    "close_demoted_when_protected", stage_row={"stage": "close", "entered_at": now_i - 100},
    opener="local", close_protection="route_pair_protected", defib_attempted=True))
scenarios.append(dead_capital_case(
    "close_demoted_when_no_attempt", stage_row={"stage": "close", "entered_at": now_i - 100},
    opener="local", close_protection=None, defib_attempted=False))

# --- identify_winners / identify_losers -----------------------------------
def _build_prof(kwargs):
    kwargs = dict(kwargs)
    revenue_val = kwargs.pop("sourced_fee_contribution_sats", None)
    role_str = kwargs.pop("channel_role", None)
    classification_str = kwargs.pop("classification", None)
    ns = types.SimpleNamespace(**kwargs)
    if revenue_val is not None:
        ns.revenue = types.SimpleNamespace(sourced_fee_contribution_sats=revenue_val)
    if role_str is not None:
        ns.channel_role = ChannelRole(role_str)
    if classification_str is not None:
        ns.classification = ProfitabilityClass(classification_str)
    return ns


def winners_case(name, channels, *, success_data=None, fee_strategy_state=None):
    db = StubDatabase(success_data=success_data, fee_strategy_state=fee_strategy_state)
    cp = make_planner(db=db)
    all_profitability = {}
    all_flow = {}
    for scid, prof_kwargs, flow_kwargs in channels:
        all_profitability[scid] = _build_prof(prof_kwargs)
        if flow_kwargs is not None:
            all_flow[scid] = types.SimpleNamespace(**flow_kwargs)
    out = cp._identify_winners(all_profitability, all_flow)
    return {"name": name, "kind": "identify_winners",
            "input": {"channels": [{"scid": s, "prof": p, "flow": f} for s, p, f in channels],
                       "success_data": success_data, "fee_strategy_state": fee_strategy_state},
            "output": out}


scenarios.append(winners_case("empty_channels", []))
scenarios.append(winners_case("basic_winner", [
    ("700000x1x0", dict(capacity_sats=1_000_000, peer_id="peerW", marginal_roi_percent=25.0),
     dict(daily_volume=600_000.0, flow_ratio=0.9, kalman_velocity=0.0, is_congested=False)),
]))
scenarios.append(winners_case("roi_below_threshold_excluded", [
    ("700000x1x0", dict(capacity_sats=1_000_000, peer_id="peerW", marginal_roi_percent=15.0),
     dict(daily_volume=600_000.0, flow_ratio=0.9, kalman_velocity=0.0, is_congested=False)),
]))
scenarios.append(winners_case("turnover_below_threshold_excluded", [
    ("700000x1x0", dict(capacity_sats=1_000_000, peer_id="peerW", marginal_roi_percent=25.0),
     dict(daily_volume=100_000.0, flow_ratio=0.9, kalman_velocity=0.0, is_congested=False)),
]))
scenarios.append(winners_case("flow_ratio_neutral_excluded", [
    ("700000x1x0", dict(capacity_sats=1_000_000, peer_id="peerW", marginal_roi_percent=25.0),
     dict(daily_volume=600_000.0, flow_ratio=0.5, kalman_velocity=0.0, is_congested=False)),
]))
scenarios.append(winners_case("no_flow_metrics_skipped", [
    ("700000x1x0", dict(capacity_sats=1_000_000, peer_id="peerW", marginal_roi_percent=25.0), None),
]))
scenarios.append(winners_case(
    "rebal_penalty_applied",
    [("700000x1x0", dict(capacity_sats=1_000_000, peer_id="peerW", marginal_roi_percent=40.0),
      dict(daily_volume=600_000.0, flow_ratio=0.9, kalman_velocity=0.0, is_congested=False))],
    success_data={"success_rate": 0.2, "total": 5},
))
scenarios.append(winners_case(
    "rebal_success_data_insufficient_total_no_penalty",
    [("700000x1x0", dict(capacity_sats=1_000_000, peer_id="peerW", marginal_roi_percent=25.0),
      dict(daily_volume=600_000.0, flow_ratio=0.9, kalman_velocity=0.0, is_congested=False))],
    success_data={"success_rate": 0.1, "total": 2},
))
scenarios.append(winners_case("velocity_and_congestion_flags", [
    ("700000x1x0", dict(capacity_sats=1_000_000, peer_id="peerW", marginal_roi_percent=25.0),
     dict(daily_volume=600_000.0, flow_ratio=0.9, kalman_velocity=0.5, is_congested=True)),
]))
scenarios.append(winners_case(
    "dts_posterior_mean_present",
    [("700000x1x0", dict(capacity_sats=1_000_000, peer_id="peerW", marginal_roi_percent=25.0),
      dict(daily_volume=600_000.0, flow_ratio=0.9, kalman_velocity=0.0, is_congested=False))],
    fee_strategy_state={"v2_state_json": json.dumps(
        {"fee_state": {"thompson_state": {"posterior_mean": 123.456}}})},
))
scenarios.append(winners_case("negative_flow_ratio_winner", [
    ("700000x1x0", dict(capacity_sats=1_000_000, peer_id="peerW", marginal_roi_percent=25.0),
     dict(daily_volume=600_000.0, flow_ratio=-0.9, kalman_velocity=0.0, is_congested=False)),
]))
scenarios.append(winners_case("channel_role_present", [
    ("700000x1x0", dict(capacity_sats=1_000_000, peer_id="peerW", marginal_roi_percent=25.0,
                         channel_role="inbound_gateway", sourced_fee_contribution_sats=500),
     dict(daily_volume=600_000.0, flow_ratio=0.9, kalman_velocity=0.0, is_congested=False)),
]))


def losers_case(name, channels, *, success_data=None, route_pairs=None, bleeders=None,
                 close_protection=None, defib_allowed=(True, "defib allowed"),
                 diag_stats=None):
    db = StubDatabase(success_data=success_data, route_pairs=route_pairs or [],
                       diag_stats=diag_stats or {"attempt_count": 0})
    cp = make_planner(db=db, bleeders=bleeders)
    cp._close_protection_reason = lambda *a, **k: close_protection
    cp._check_defib_allowed = lambda peer_id: defib_allowed
    all_profitability = {}
    all_flow = {}
    for scid, prof_kwargs, flow_kwargs in channels:
        all_profitability[scid] = _build_prof(prof_kwargs)
        if flow_kwargs is not None:
            all_flow[scid] = types.SimpleNamespace(**flow_kwargs)
    out = cp._identify_losers(all_profitability, all_flow)
    return {"name": name, "kind": "identify_losers",
            "input": {"channels": [{"scid": s, "prof": p, "flow": f} for s, p, f in channels],
                       "success_data": success_data, "close_protection": close_protection,
                       "defib_allowed": list(defib_allowed), "diag_stats": diag_stats,
                       "bleeders_hard": bool(bleeders)},
            "output": out}


scenarios.append(losers_case("empty_channels_losers", []))
scenarios.append(losers_case(
    "zombie_fire_sale_close",
    [("700000x1x0", dict(capacity_sats=500_000, peer_id="peerL", marginal_roi_percent=-60.0,
                          roi_percent=-70.0, classification="zombie", days_open=100,
                          opener="local", marginal_profit_30d_sats=-200),
      dict(flow_ratio=0.0, capacity=500_000, daily_volume=1000.0, kalman_regime_change=False))],
    diag_stats={"attempt_count": 2},
))
scenarios.append(losers_case(
    "underwater_deep_fire_sale",
    [("700000x1x0", dict(capacity_sats=500_000, peer_id="peerL", marginal_roi_percent=-60.0,
                          roi_percent=-70.0, classification="underwater", days_open=100,
                          opener="local", marginal_profit_30d_sats=-200),
      dict(flow_ratio=0.0, capacity=500_000, daily_volume=1000.0, kalman_regime_change=False))],
    diag_stats={"attempt_count": 2},
))
scenarios.append(losers_case(
    "underwater_not_deep_enough_not_fire_sale",
    [("700000x1x0", dict(capacity_sats=500_000, peer_id="peerL", marginal_roi_percent=-30.0,
                          roi_percent=-30.0, classification="underwater", days_open=100,
                          opener="local", marginal_profit_30d_sats=-50),
      dict(flow_ratio=0.0, capacity=500_000, daily_volume=1000.0, kalman_regime_change=False))],
))
scenarios.append(losers_case(
    "days_open_too_young_not_fire_sale",
    [("700000x1x0", dict(capacity_sats=500_000, peer_id="peerL", marginal_roi_percent=-60.0,
                          roi_percent=-70.0, classification="zombie", days_open=50,
                          opener="local", marginal_profit_30d_sats=-200),
      dict(flow_ratio=0.0, capacity=500_000, daily_volume=1000.0, kalman_regime_change=False))],
))
scenarios.append(losers_case(
    "stagnant_balanced_low_turnover",
    [("700000x1x0", dict(capacity_sats=1_000_000, peer_id="peerL", marginal_roi_percent=5.0,
                          roi_percent=5.0, classification="break_even", days_open=40,
                          opener="local", marginal_profit_30d_sats=10),
      dict(flow_ratio=0.05, capacity=1_000_000, daily_volume=100.0, kalman_regime_change=False))],
    diag_stats={"attempt_count": 0},
))
scenarios.append(losers_case(
    "stagnant_high_roi_not_loser",
    [("700000x1x0", dict(capacity_sats=1_000_000, peer_id="peerL", marginal_roi_percent=15.0,
                          roi_percent=15.0, classification="break_even", days_open=40,
                          opener="local", marginal_profit_30d_sats=10),
      dict(flow_ratio=0.05, capacity=1_000_000, daily_volume=100.0, kalman_regime_change=False))],
))
scenarios.append(losers_case(
    "hard_bleeder_bypasses_defib_gate",
    [("700000x1x0", dict(capacity_sats=1_000_000, peer_id="peerL", marginal_roi_percent=5.0,
                          roi_percent=5.0, classification="break_even", days_open=40,
                          opener="local", marginal_profit_30d_sats=10),
      dict(flow_ratio=0.05, capacity=1_000_000, daily_volume=100.0, kalman_regime_change=False))],
    diag_stats={"attempt_count": 0},
    bleeders=[types.SimpleNamespace(channel_id="700000x1x0", is_hard_bleeder=True)],
))
scenarios.append(losers_case(
    "close_protection_skips_channel",
    [("700000x1x0", dict(capacity_sats=500_000, peer_id="peerL", marginal_roi_percent=-60.0,
                          roi_percent=-70.0, classification="zombie", days_open=100,
                          opener="local", marginal_profit_30d_sats=-200),
      dict(flow_ratio=0.0, capacity=500_000, daily_volume=1000.0, kalman_regime_change=False))],
    diag_stats={"attempt_count": 2},
    close_protection="inbound_gateway_protected",
))
scenarios.append(losers_case(
    "remote_opener_shallow_underwater_skipped",
    [("700000x1x0", dict(capacity_sats=500_000, peer_id="peerL", marginal_roi_percent=-60.0,
                          roi_percent=-70.0, classification="underwater", days_open=100,
                          opener="remote", marginal_profit_30d_sats=-200),
      dict(flow_ratio=0.0, capacity=500_000, daily_volume=1000.0, kalman_regime_change=False))],
    diag_stats={"attempt_count": 2},
))
scenarios.append(losers_case(
    "remote_opener_deeply_underwater_not_skipped",
    [("700000x1x0", dict(capacity_sats=500_000, peer_id="peerL", marginal_roi_percent=-80.0,
                          roi_percent=-90.0, classification="underwater", days_open=100,
                          opener="remote", marginal_profit_30d_sats=-500),
      dict(flow_ratio=0.0, capacity=500_000, daily_volume=1000.0, kalman_regime_change=False))],
    diag_stats={"attempt_count": 2},
))
scenarios.append(losers_case(
    "regime_change_demotes_close_to_defibrillate",
    [("700000x1x0", dict(capacity_sats=500_000, peer_id="peerL", marginal_roi_percent=-60.0,
                          roi_percent=-70.0, classification="zombie", days_open=100,
                          opener="local", marginal_profit_30d_sats=-200),
      dict(flow_ratio=0.0, capacity=500_000, daily_volume=1000.0, kalman_regime_change=True))],
    diag_stats={"attempt_count": 2},
))
scenarios.append(losers_case(
    "defib_policy_blocked_forces_close",
    [("700000x1x0", dict(capacity_sats=1_000_000, peer_id="peerL", marginal_roi_percent=5.0,
                          roi_percent=5.0, classification="break_even", days_open=40,
                          opener="local", marginal_profit_30d_sats=10),
      dict(flow_ratio=0.05, capacity=1_000_000, daily_volume=100.0, kalman_regime_change=False))],
    diag_stats={"attempt_count": 0},
    defib_allowed=(False, "rebalance_mode=disabled forbids filling — defib blocked"),
))
scenarios.append(losers_case(
    "rebal_difficulty_hard_promotes_stagnant_to_fire_sale",
    [("700000x1x0", dict(capacity_sats=1_000_000, peer_id="peerL", marginal_roi_percent=5.0,
                          roi_percent=5.0, classification="break_even", days_open=40,
                          opener="local", marginal_profit_30d_sats=10),
      dict(flow_ratio=0.05, capacity=1_000_000, daily_volume=100.0, kalman_regime_change=False))],
    diag_stats={"attempt_count": 0},
    success_data={"success_rate": 0.1, "total": 5},
))


# --- score_candidate --------------------------------------------------------
def score_candidate_case(name, base_score, *, reputation=None, closed_summary=None,
                          uptime=None, node_addresses=None, inbound_fee_data=None,
                          dest_channels=None, sink_adjacent=False, demand_flow_role=None):
    db = StubDatabase()
    db.peer_reputation = reputation
    db.closed_summary = closed_summary
    db.uptime_pct = uptime
    db.inbound_fee_data = inbound_fee_data

    def get_node_info(peer_id):
        if node_addresses is None:
            return {"nodes": []}
        return {"nodes": [{"addresses": node_addresses}]}

    def get_channels(destination=None, source=None):
        return {"channels": dest_channels or []}

    ds = types.SimpleNamespace(get_node_info=get_node_info, get_channels=get_channels)
    cp = make_planner(db=db, data_service=ds)
    if sink_adjacent:
        cp._demand_flow_sink_adjacent = {"peerX"}
    if demand_flow_role is not None:
        cp._demand_flow_profiles = {"peerX": types.SimpleNamespace(role=demand_flow_role)}
    score = cp._score_candidate("peerX", base_score)
    return {"name": name, "kind": "score_candidate",
            "input": {"base_score": base_score, "reputation": reputation, "closed_summary": closed_summary,
                       "uptime": uptime, "node_addresses": node_addresses, "inbound_fee_data": inbound_fee_data,
                       "dest_channels": dest_channels, "sink_adjacent": sink_adjacent,
                       "demand_flow_role": demand_flow_role},
            "output": score}


scenarios.append(score_candidate_case("no_signals_unchanged", 0.5))
scenarios.append(score_candidate_case("reputation_boost", 0.5, reputation={"successes": 9, "failures": 1}))
scenarios.append(score_candidate_case("reputation_poor", 0.5, reputation={"successes": 0, "failures": 9}))
scenarios.append(score_candidate_case("closed_channel_profit_boost", 0.5, closed_summary={"marginal_roi_proxy": 5.0}))
scenarios.append(score_candidate_case("uptime_penalty", 0.5, uptime=50.0))
scenarios.append(score_candidate_case("uptime_high_no_penalty", 0.5, uptime=95.0))
scenarios.append(score_candidate_case("clearnet_boost", 0.5, node_addresses=[{"type": "ipv4"}]))
scenarios.append(score_candidate_case("tor_only_no_boost", 0.5, node_addresses=[{"type": "torv3"}]))
scenarios.append(score_candidate_case("inbound_fee_penalty", 0.5, inbound_fee_data={"median_fee_ppm": 700}))
scenarios.append(score_candidate_case("inbound_fee_below_threshold_no_penalty", 0.5, inbound_fee_data={"median_fee_ppm": 150}))
scenarios.append(score_candidate_case("large_channel_bonus", 0.5, dest_channels=[
    {"active": True, "amount_msat": 15_000_000_000}]))
scenarios.append(score_candidate_case("medium_channel_bonus", 0.5, dest_channels=[
    {"active": True, "amount_msat": 6_000_000_000}]))
scenarios.append(score_candidate_case("sink_adjacent_boost", 0.5, sink_adjacent=True))
scenarios.append(score_candidate_case("demand_flow_sink_role_boost", 0.5, demand_flow_role="sink"))
scenarios.append(score_candidate_case("demand_flow_source_role_boost", 0.5, demand_flow_role="source"))
scenarios.append(score_candidate_case("demand_flow_unknown_role_penalty", 0.5, demand_flow_role="unknown"))
scenarios.append(score_candidate_case(
    "all_signals_combined", 0.5,
    reputation={"successes": 9, "failures": 1}, closed_summary={"marginal_roi_proxy": 5.0},
    uptime=50.0, node_addresses=[{"type": "ipv4"}], inbound_fee_data={"median_fee_ppm": 700},
    dest_channels=[{"active": True, "amount_msat": 15_000_000_000}], sink_adjacent=True,
))


# --- discover_from_winners ---------------------------------------------------
def discover_winners_case(name, winners):
    cp = make_planner()
    out = cp._discover_from_winners(winners)
    return {"name": name, "kind": "discover_from_winners", "input": {"winners": winners}, "output": out}


scenarios.append(discover_winners_case("empty_winners", []))
scenarios.append(discover_winners_case("below_threshold_excluded", [
    {"peer_id": "p1", "roi": 25.0, "scid": "700000x1x0"}]))
scenarios.append(discover_winners_case("above_threshold_included", [
    {"peer_id": "p1", "roi": 45.0, "scid": "700000x1x0"}]))
scenarios.append(discover_winners_case("mixed_winners", [
    {"peer_id": "p1", "roi": 45.0, "scid": "700000x1x0"},
    {"peer_id": "p2", "roi": 20.0, "scid": "700000x2x0"},
    {"peer_id": "p3", "roi": 90.0, "scid": "700000x3x0"},
]))


# --- discover_from_graph ------------------------------------------------------
def discover_graph_case(name, cached_source_channels, existing_peer_ids, our_node_id="us"):
    cp = make_planner()
    cp._cycle_channels_source = cached_source_channels
    ds = types.SimpleNamespace(get_node_id=lambda: our_node_id)
    cp.data_service = ds
    out = cp._discover_from_graph(set(existing_peer_ids))
    return {"name": name, "kind": "discover_from_graph",
            "input": {"cached_source_channels": cached_source_channels,
                       "existing_peer_ids": existing_peer_ids, "our_node_id": our_node_id},
            "output": out}


scenarios.append(discover_graph_case("empty_cache", {}, []))
scenarios.append(discover_graph_case("below_channel_count_excluded", {
    "hub1": [{"active": True, "amount_msat": 1_000_000_000}] * 3,
}, []))
scenarios.append(discover_graph_case("meets_channel_count_included", {
    "hub1": [{"active": True, "amount_msat": 1_000_000_000}] * 6,
}, []))
scenarios.append(discover_graph_case("our_node_excluded", {
    "us": [{"active": True, "amount_msat": 1_000_000_000}] * 6,
}, [], our_node_id="us"))
scenarios.append(discover_graph_case("existing_peer_excluded", {
    "hub1": [{"active": True, "amount_msat": 1_000_000_000}] * 6,
}, ["hub1"]))
scenarios.append(discover_graph_case("inactive_channels_not_counted", {
    "hub1": [{"active": False, "amount_msat": 1_000_000_000}] * 6,
}, []))
scenarios.append(discover_graph_case("ranked_by_score_desc", {
    "small_hub": [{"active": True, "amount_msat": 100_000_000}] * 5,
    "big_hub": [{"active": True, "amount_msat": 5_000_000_000}] * 10,
}, []))


# --- demand_flow: classify_peers / find_sink_adjacent_candidates -----------
def classify_peers_case(name, flows):
    classifier = DemandFlowClassifier()
    all_flow = {}
    for scid, peer_id, sats_in, sats_out in flows:
        all_flow[scid] = types.SimpleNamespace(peer_id=peer_id, sats_in=sats_in, sats_out=sats_out)
    profiles = classifier.classify_peers(all_flow)
    out = {pid: {"role": p.role, "confidence": p.confidence, "net_flow_ratio": p.net_flow_ratio}
           for pid, p in profiles.items()}
    return {"name": name, "kind": "classify_peers", "input": {"flows": flows}, "output": out}


scenarios.append(classify_peers_case("empty_flows", []))
scenarios.append(classify_peers_case("zero_flow_unknown", [("700000x1x0", "peerA", 0, 0)]))
scenarios.append(classify_peers_case("source_dominant", [("700000x1x0", "peerA", 900_000, 100_000)]))
scenarios.append(classify_peers_case("sink_dominant", [("700000x1x0", "peerA", 100_000, 900_000)]))
scenarios.append(classify_peers_case("router_balanced", [("700000x1x0", "peerA", 500_000, 500_000)]))
scenarios.append(classify_peers_case("high_volume_confidence_capped", [("700000x1x0", "peerA", 5_000_000_000, 0)]))
scenarios.append(classify_peers_case("multi_channel_same_peer_aggregated", [
    ("700000x1x0", "peerA", 100_000, 0), ("700000x2x0", "peerA", 0, 900_000)]))


def sink_adjacent_case(name, sink_profiles, sink_channels, existing_peers):
    classifier = DemandFlowClassifier()
    profiles = {pid: types.SimpleNamespace(node_id=pid, role="sink", confidence=conf, net_flow_ratio=ratio)
                for pid, conf, ratio in sink_profiles}
    out = classifier.find_sink_adjacent_candidates(profiles, sink_channels, set(existing_peers))
    return {"name": name, "kind": "find_sink_adjacent_candidates",
            "input": {"sink_profiles": sink_profiles, "sink_channels": sink_channels,
                       "existing_peers": existing_peers},
            "output": out}


scenarios.append(sink_adjacent_case("no_sinks", [], {}, []))
scenarios.append(sink_adjacent_case("one_sink_one_candidate", [
    ("sinkA", 0.5, -0.6)], {"sinkA": [{"destination": "cand1", "active": True}]}, []))
scenarios.append(sink_adjacent_case("existing_peer_excluded", [
    ("sinkA", 0.5, -0.6)], {"sinkA": [{"destination": "cand1", "active": True}]}, ["cand1"]))
scenarios.append(sink_adjacent_case("inactive_channel_excluded", [
    ("sinkA", 0.5, -0.6)], {"sinkA": [{"destination": "cand1", "active": False}]}, []))
scenarios.append(sink_adjacent_case("multiple_candidates_ranked", [
    ("sinkA", 0.5, -0.6), ("sinkB", 0.2, -0.4)],
    {"sinkA": [{"destination": "cand1", "active": True}],
     "sinkB": [{"destination": "cand2", "active": True}]}, []))
scenarios.append(sink_adjacent_case("dedup_first_sink_wins", [
    ("sinkA", 0.9, -0.9), ("sinkB", 0.1, -0.1)],
    {"sinkA": [{"destination": "cand1", "active": True}],
     "sinkB": [{"destination": "cand1", "active": True}]}, []))


# --- size_channel -------------------------------------------------------------
def size_channel_case(name, candidate, all_candidates, available_sats, *,
                       min_ch=500_000, max_ch=10_000_000, dest_channels=None):
    def get_channels(destination=None, source=None):
        return {"channels": dest_channels or []}
    ds = types.SimpleNamespace(get_channels=get_channels)
    cp = make_planner(data_service=ds)
    c = cfg(planner_min_channel_sats=min_ch, planner_max_channel_sats=max_ch)
    size = cp._size_channel(candidate, all_candidates, available_sats, c)
    return {"name": name, "kind": "size_channel",
            "input": {"candidate": candidate, "all_candidates": all_candidates,
                       "available_sats": available_sats, "min_ch": min_ch, "max_ch": max_ch,
                       "dest_channels": dest_channels},
            "output": size}


scenarios.append(size_channel_case("no_candidates_uses_min", {"peer_id": "p1", "score": 0.5}, [], 5_000_000))
scenarios.append(size_channel_case(
    "proportional_two_candidates",
    {"peer_id": "p1", "score": 0.5},
    [{"peer_id": "p1", "score": 0.5}, {"peer_id": "p2", "score": 0.5}],
    4_000_000,
))
scenarios.append(size_channel_case(
    "never_more_than_half_available",
    {"peer_id": "p1", "score": 1.0},
    [{"peer_id": "p1", "score": 1.0}],
    4_000_000,
))
scenarios.append(size_channel_case(
    "competitive_floor_bump",
    {"peer_id": "p1", "score": 0.1},
    [{"peer_id": "p1", "score": 0.1}, {"peer_id": "p2", "score": 0.9}],
    20_000_000,
    dest_channels=[{"active": True, "amount_msat": 16_000_000_000}],
))
scenarios.append(size_channel_case(
    "clamped_to_max",
    {"peer_id": "p1", "score": 1.0},
    [{"peer_id": "p1", "score": 1.0}],
    100_000_000,
    max_ch=3_000_000,
))
scenarios.append(size_channel_case(
    "clamped_to_min",
    {"peer_id": "p1", "score": 0.01},
    [{"peer_id": "p1", "score": 0.01}, {"peer_id": "p2", "score": 100.0}],
    2_000_000,
    min_ch=500_000,
))

out = {"scenarios": scenarios}
print(json.dumps(out, indent=2, sort_keys=True, default=lambda o: None if isinstance(o, float) and o != o else o))
