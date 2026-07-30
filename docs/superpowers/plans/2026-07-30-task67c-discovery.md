# Task 67c: Close the Planner's Open-Side Gaps

> Operator-directed 2026-07-30: *"Close the planner gap first. There should
> be no gaps at all in any aspect of the port. Full behavioural parity."*

**Goal:** Close the six remaining capital gaps so the planner OPENS
channels, not just defibrillates and closes.

## Remaining fields

| Field | Source |
|---|---|
| `discovery` (`DiscoveryEvidence`) | the CLN gossip graph + route-pair rows + my profitability |
| `candidate_enrichment` | production DB: reputation, closed-channel profit, uptime, inbound fees; `listnodes` for clearnet |
| `open_candidate_evidence` | per-peer dest capacities + an open-EV template + enrichment |
| `dual_fund_peers` | `listnodes` feature bits |
| `recycle_candidates` | existing channels + closure costs |
| `redeployment_winner_evs` | open EV per winner peer |

## Why this is tractable

Two things already exist and are proven:

1. **The full `listchannels` graph prefetch** — `fee_evidence.rs:162`
   already fetches the whole gossip graph once per cycle for the fee loop.
   Discovery needs the same data; it does NOT need a new fetch layer.
2. **The five discovery strategies are FROZEN and ported** —
   `discover_from_winners`, `_neighbors`, `_graph`, `_route_pairs`,
   `_demand_flow`, plus the capital-efficiency variant. None of them is
   being rewritten.

And `PatronCandidate` is just `{peer_id, marginal_roi_percent}`, which task
67b's profitability assembler already produces.

So this is again evidence ASSEMBLY: group one graph fetch by source and
destination, project profitability into patrons, read the enrichment
columns, and hand the frozen strategies their inputs.

## Slices

### Slice 1 — graph evidence assembly
Group the single `listchannels` array into
`neighbor_patron_source_channels`, `graph_cached_source_channels`,
`route_peer_source_channels` and `channel_to_peer`; build `all_channels`
patrons from profitability. RED: grouping is by SOURCE for patron
lookups and the scid→peer map is built from both endpoints; an unreadable
graph REFUSES rather than yielding an empty graph (which the frozen
strategies would read as "no candidates anywhere").

### Slice 2 — enrichment assembly
Reputation, closed-channel profit, uptime %, clearnet address, inbound
median fee, dest capacities. Each optional field distinguishes ABSENT from
zero, since `Option<f64>` vs `Some(0.0)` changes scoring.

### Slice 3 — open candidates, dual-fund peers, redeployment EVs
Build `OpenCandidateEvidence` per candidate and the open-EV template; read
dual-fund support from `listnodes` feature bits; compute redeployment EVs
per winner peer.

### Slice 4 — recycle candidates
Existing channels plus closure-cost evidence.

### Slice 5 — wire, mutate, report
Fill all six in `capital_evidence.rs`, DELETE `ANALYTICS_GAP` entirely
(the gap list should end empty), mutation matrix, report.

## Non-negotiables

- The frozen discovery strategies are NOT modified.
- An unreadable source REFUSES. The strategies are total over empty
  inputs, so a silently-empty graph produces a confident "no candidates"
  indistinguishable from a healthy quiet cycle — the same failure shape
  task 67b guarded against.
- Absent vs zero stays distinct in every enrichment field.
