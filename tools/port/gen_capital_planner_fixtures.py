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
        return None

    def record_planner_action(self, **kwargs):
        return 1

    def update_planner_action(self, action_id, status):
        pass


class StubProfitability:
    def __init__(self, database):
        self.database = database


def make_planner(db=None, data_service=None):
    db = db or StubDatabase()
    profitability = StubProfitability(db)
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

out = {"scenarios": scenarios}
print(json.dumps(out, indent=2, sort_keys=True, default=lambda o: None if isinstance(o, float) and o != o else o))
