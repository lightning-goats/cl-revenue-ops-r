# Task 67b: Fill the Analytics Gaps for True Behavioural Parity

> Operator-directed 2026-07-30: "Fill the gaps. We want true behavioural
> parity." Prerequisite for the whole-plugin cutover.

**Goal:** Make the capital planner actually plan and the Boltz auto-cycle
actually select, by filling Task 62's eleven `EvidenceGap` fields and Task
63's treasury/balance candidate analytics.

## Why this is smaller than it looks

The decision KERNELS are already ported and frozen:
`revops_capital::planner::winners::identify_winners`,
`::losers::identify_losers`, and `revops_analytics::profitability`'s
`ChannelProfitability` / `classify_channel`. None of that is being
rewritten.

What is missing is **evidence assembly**: building one
`ChannelProfitability` per channel from the Rust-owned reads, then
projecting it into `WinnerCandidateEvidence` / `LoserChannelEvidence` and
handing those to the frozen kernels.

Python's `profitability_analyzer.py` is 2,761 lines, but most of that is
its TTL cache, stampede lock, and RPC service layer — none of which this
port needs, because the Rust side already has a serialized owner and its
own store.

## Confirmed data sources (production DB, read-only)

| Need | Source |
|---|---|
| `open_cost_sats`, `capacity_sats`, `opened_at` → `days_open` | `channel_costs` (channel_id, peer_id, open_cost_sats, capacity_sats, opened_at) |
| `rebalance_cost_sats`, 30d window | `rebalance_costs` (channel_id, cost_sats, cost_msat, timestamp) |
| `fees_earned_msat`, `volume_routed_msat`, `forward_count` | `forwards` WHERE `out_channel = scid` — the EXIT channel earns the fee |
| `sourced_*` (entry-side attribution) | `forwards` WHERE `in_channel = scid` — for protection/valuation only, NEVER summed into fleet revenue (double-count) |
| flow evidence (`flow_ratio`, `daily_volume`, `kalman_velocity`, `is_congested`) | Task 67's `rust_channel_flow_states` |
| `dts_posterior_mean` | `v2_state_json` → `thompson_state.posterior_mean` |
| closure cost | `channel_closure_costs` |

## Slices

### Slice 1 — per-channel profitability queries (revops-db)
`per_channel_revenue(since)` and `per_channel_costs()` returning maps keyed
by scid, plus the 30-day windowed variants. Exit-side vs entry-side
attribution kept strictly separate. RED: round-trips over a temp DB with a
forward whose in and out channels differ, asserting the fee lands on the
EXIT channel and appears as `sourced` on the ENTRY channel — the
double-count trap.

### Slice 2 — profitability assembler (revops)
Assemble `ChannelProfitability` per channel and run the frozen
`classify_channel`. Every required read `Result`-shaped; a failed read is a
typed refusal, never a zeroed channel (a zero-revenue channel and an
unreadable one must not look alike).

### Slice 3 — winners/losers evidence + the eleven gaps
Project profitability + flow state into `WinnerCandidateEvidence` /
`LoserChannelEvidence`, call the frozen kernels, and fill
`capital_evidence.rs`'s eleven fields. Delete the `ANALYTICS_GAP` const and
each gap as it closes rather than editing its text.

### Slice 4 — Boltz candidate analytics
Treasury status (on-chain balance vs target) and balance recommendations,
so `select_boltz_auto_cycle_mode` sees real candidates.

**This makes Task 63's deliberately-dead execution branch LIVE.** That
branch is documented "kept total, unreachable until Task 67". Turning a
dead money-moving path live is the point of this task, but it must be
called out in the report and re-reviewed, not slipped in.

### Slice 5 — mutations, battery, report
Matrix B1–B8 covering: fee attributed to the entry channel (double-count),
a failed read zeroing a channel, marginal-ROI using total instead of 30d
cost, winners/losers thresholds bypassed, the eleven gaps re-emptied, Boltz
mode selection ignoring candidates.

## Non-negotiables

- The frozen kernels are NOT modified. If a kernel looks wrong, that is a
  finding to report, not a patch to apply.
- Absent vs zero stays distinct everywhere.
- No live contact; temp DBs and fixtures only.
