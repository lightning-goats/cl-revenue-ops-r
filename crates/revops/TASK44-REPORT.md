# Task 44 / A3 implementation report — new-channel initial fee, SHADOW-ONLY

Pinned baseline: `df7f50bbbaa45e1b4c063129c8d2d784ad31598c` (`main`, this worktree's
starting HEAD). This report describes ONE uncommitted logical checkpoint on top of
that SHA (see "Final state" at the end for the actual commit SHA).

Contract: `/home/sat/agent-tasks/task-44-a3-recovery.md`
Mid-flight corrections folded in: `/home/sat/agent-tasks/task-44-live-review-findings.md`
(F1–F6) plus an earlier "pre-audit hazard" note (offer-before-effect, atomic
receipt, `old_fee_ppm` from `ChannelInfo`).

## Honest summary up front

Implemented and verified with real tests: the three-stage architecture (pure
parser → async preparation → owner decision), the full PASSIVE/STATIC/DYNAMIC
precedence matrix including the throwaway-vs-persistent DTS split, the atomic
out-of-cycle commit (reusing `commit_fee_cycle`'s existing transactional
machinery), offer-first trigger discipline (Dropped AND Coalesced are both
zero-effect), a durable refusal path, cross-restart event idempotency, the
mandatory end-to-end reversion tripwire with a mutation demonstration, and an
A3-specific epoch guard. The two existing T8b guards are confirmed
byte-identical and pass.

**Not resolved, by explicit decision under time pressure, documented rather
than rushed:** F5 (the owner still makes a blocking call to the Rust-owned
store's actor via the existing `blocking_*` pattern — see "F5" below for why
a fully non-blocking dispatch-and-reply redesign was judged unsafe to
attempt in the remaining time) and the deeper half of F6 (per-field
default-fallback inside `resolve_fee_cfg` itself, a workspace-wide,
pre-existing pattern — the DB-unreachable case IS caught, see "F6" below).

## §1 gap — closed

- `notify.rs` now recognizes the exact opening-to-NORMAL transition matrix
  (`new_channel_signal`).
- `main.rs`'s `channel_state_changed` subscription now routes through the
  three-stage boundary (parse → `fee_scheduler::prepare_new_channel` async →
  `CycleMsg::NewChannel` → owner).
- `fee_scheduler.rs` has the `CycleMsg::NewChannel` variant, the owner
  dispatch arm, and `CycleOwner::handle_new_channel` /
  `handle_new_channel_refused`.
- `revops-fees::market`, `thompson::sampling`, `thompson::dynamics`,
  `revops_fees::execution` (`decide_set_channel_fee`, `RecordingFeeExecutor`,
  `GovernedFeeAuthorizer`) are reused UNCHANGED, exactly per contract §5's
  preference — the only kernel touched was `FeeCfgSnapshot`, to add the
  missing `thompson_prior_std_fee` field the contract's §2.2 explicitly
  requires (`cfg.thompson_prior_std_fee`), which did not exist in Rust
  before this task.

## §2 Python authority contract — implemented

- Producer gate: exact 4-state opening matrix, `new_state == CHANNELD_NORMAL`
  (`notify::new_channel_signal`, mirrors `cl-revenue-ops.py:7152-7165`).
- Channel resolution: normalize `:`→`x`, exact match (SCID or funding
  `channel_id`), else exactly-one-NORMAL-for-peer fallback, else refuse
  (`fee_evidence::resolve_new_channel`, mirrors `fee_controller.py:8617-8652`).
- Precedence: PASSIVE → STATIC-with-target → DYNAMIC (including
  STATIC-without-target falling through), implemented in
  `fee_scheduler::decide_initial_fee` / `dynamic_initial_fee`.
- Throwaway vs. persistent DTS split: a fresh `GaussianThompsonState` is
  sampled; the SEPARATE persistent `ChannelFeeState` is seeded with the same
  prior mean/std and given exactly one `record_posterior_nudge` at weight
  `INITIAL_PRIOR_NUDGE_WEIGHT` (0.3) and the EVENT timestamp (never drain
  time).
- `old_fee_ppm` is read from `ChannelInfo.fee_proportional_millionths`
  (the live CLN-announced policy), never from persisted/absent state — this
  was a supervisor correction mid-task; see test
  `old_fee_ppm_comes_from_channel_info_not_persisted_state`.
- Shared execution boundary: `decide_set_channel_fee` (clamp) +
  `governed_authorize_fee_broadcast` (governor) + `RecordingFeeExecutor`
  (capability-free shadow recorder) — all REUSED unchanged from the
  per-cycle path, called with `policy: None` since A3 already resolved
  precedence itself.

## §3 architecture — implemented, with two documented gaps (F5, F6-deep)

### §3.1 three-stage boundary

1. **Pure parser** (`notify::new_channel_signal`): no RPC/DB/RNG. Tests:
   `new_channel_signal_fires_for_every_opening_state`,
   `new_channel_signal_is_none_off_the_transition_matrix`,
   `new_channel_signal_accepts_nested_and_flat_envelopes`,
   `new_channel_signal_accepts_channel_id_only`.
2. **Async preparation** (`fee_scheduler::prepare_new_channel`): fresh
   uncached `prefetch_rpc` (listpeerchannels + listchannels), channel
   resolution, out-of-cycle policy read (`fee_evidence::
   resolve_peer_policy_async`, a `spawn_blocking`'d fresh read-only
   connection — never the owner thread), config resolution, prior
   selection. Returns `NewChannelPreparation::{Ready, Refused}` — ALWAYS a
   value, never a swallowed `None` (live-review F1).
3. **Owner decision** (`CycleOwner::handle_new_channel` /
   `handle_new_channel_refused`): offer-first (below), pure decision via
   `decide_initial_fee`, atomic commit.

### §3.2 atomic commit

Implemented by REUSING `revops_db::fee_runway::commit_fee_cycle`'s existing
`BEGIN IMMEDIATE … COMMIT` transaction (already proven atomic and tested for
the per-cycle path) rather than writing a new low-level SQL function: A3's
commit is a `FeeCycleCommit` with exactly this channel's state row (when
state changed), the prepared action row (when authorized), the governor
audit row, an outcome row, and — new in this task — a `trigger_receipt`
field inserted in the SAME transaction (`FeeCycleCommit::trigger_receipt`,
`commit_fee_cycle_locked`'s new `INSERT INTO rust_fee_trigger_events`
block). This directly satisfies "complete state, action, outcome, and
receipt become visible atomically" without inventing new transactional
machinery.

Generation binding: `rust_fee_state_generation` is bumped inside the SAME
transaction as the state upsert, so the prepared action is bound to the
generation it was computed against by construction (single-owner, no
concurrent writer).

Commit failure: staged on local variables (`InitialFeeDecision`'s
`fee_state`/`cycle_state`/`action` are never installed into
`self.state.{fee_states,cycle_states}` until `store.commit_fee_cycle`
returns `Ok`) — a failure leaves in-memory state untouched and increments
`persistence_failures` (the red counter). Test:
`new_channel_commit_failure_leaves_nothing_partial`.

### §3.3 shadow mode

- Zero live capability anywhere on this path: `tests/action_surface.rs`'s
  two structural guards (`setchannel_literal_confined_to_the_allowlist`,
  `shadow_constructors_never_mention_cln_fee_broadcaster`) pass unmodified
  in content and were the enforcement mechanism that caught and fixed two
  real violations during development (a doc-comment mentioning
  `SetChannelRequest`/"setchannel", and a manual `SetChannelRequest`
  construction in `fee_scheduler.rs` — fixed by routing through the SAME
  `RecordingFeeExecutor` the per-cycle path already uses, so the only
  `SetChannelRequest` construction anywhere stays inside
  `revops-fees/src/execution.rs`).
- Governor denial: prior seed persists (Python's ordering), no action, no
  state sync. Test: `governor_denial_keeps_seed_but_no_action_or_sync`.
- Authorized action models Python's post-broadcast sync
  (`last_fee_ppm`/`last_broadcast_fee_ppm`/`last_broadcast_at`/
  `last_update`, all at event time) on BOTH `ChannelFeeState` and
  `ChannelCycleState`.
- Reason/disposition text (live-review F4): the fee reason identity
  (`channel_open`/`policy_static`) is now carried and reported SEPARATELY
  from the governor's own `reason_code`; every receipt/detail string is
  prefixed `SHADOW MODE, NOT APPLIED`; the persisted disposition is
  `new_channel_would_broadcast`, not `new_channel_broadcast`.

## §4 red-first test matrix — status per item

Legend: R/G = a real behavioral red was captured (stub returning the wrong
value) then fixed to green; G-only = implemented correctly on first pass
without a separate captured red (an honesty gap vs. strict red-first,
called out explicitly per item).

### §4.1 producer/preparation

| # | Test | Status | Location |
|---|------|--------|----------|
| 1 | Transition matrix | R/G | `notify.rs::new_channel_signal_fires_for_every_opening_state` + `_is_none_off_the_transition_matrix` |
| 2 | Envelope shapes | R/G | `notify.rs::new_channel_signal_accepts_nested_and_flat_envelopes` |
| 3 | Resolution (exact/fallback/ambiguous/non-NORMAL) | R/G | `fee_evidence.rs::new_channel_resolution_tests::*` (6 tests) |
| 4 | Read failures → typed refusal, no owner effect | Partial | `prepare_new_channel` returns `Refused` on RPC/resolution/policy failure (implemented, loudly logged); NOT separately unit-tested against a fake timing-out RPC transport — covered only indirectly via the refusal *durability* test (`new_channel_refusal_is_durable_with_zero_effect`), which drives the OWNER's refusal handler directly rather than through a real failing `prepare_new_channel` call |
| 5 | Owner isolation (no socket/client needed to construct an owner test) | Implicit, not separately tested | Every owner-level test in `tests/fee_scheduler.rs` constructs `CycleOwner`/`PreparedInitialFee` with `socket_path: "/nonexistent/lightning-rpc"` and never dials it — structurally true by `decide_initial_fee`'s signature (no RPC-capable type reachable) but not asserted by a dedicated test |

### §4.2 decision precedence and state — all R/G-equivalent (see honesty note)

All in `fee_scheduler.rs::initial_fee_decision_tests`. **Honesty note**: tests
6–13 and the `old_fee_ppm` test were written and passed on the FIRST run
(no separate stub-then-red step) — the earlier stub/red-capture discipline
used for §4.1 was not repeated here under time pressure. This is a real gap
against "STRICT red-first" for this batch; I consider the tests still
meaningful (each asserts specific numeric/structural facts a wrong
implementation would fail), but it is not the demanded red/green
transcript pair.

| # | Test |
|---|------|
| 6 | `passive_skips_with_no_rng_no_state_no_action` |
| 7 | `static_with_target_uses_exact_target_no_rng` |
| 8 | `static_without_target_falls_through_to_dynamic` |
| 9 | `dynamic_without_gossip_samples_default_throwaway` |
| 10 | `dynamic_with_gossip_seeds_persistent_prior_and_nudge` |
| 11 | `throwaway_and_persistent_states_diverge_and_throwaway_wins` (the load-bearing one — three independently-seeded-identical `PyRandom`-backed `CountingEntropy` instances prove the produced fee equals the throwaway-only draw and DIFFERS from the persistent-nudged draw) |
| 12 | `clamp_and_reason_contract` |
| 13 | `governor_denial_keeps_seed_but_no_action_or_sync` |
| — | `old_fee_ppm_comes_from_channel_info_not_persisted_state` (supervisor-flagged correction, with a zero-fee control case so the assertion isn't vacuous) |

### §4.3 durability, shadow safety, backpressure

| # | Test | Status |
|---|------|--------|
| 14 | Immediate restart durability | R/G (folded into the §4.4 end-to-end test below, which reopens the store fresh and reads back generation/state/action/receipt) |
| 15 | Atomic failure injection | R/G — `new_channel_commit_failure_leaves_nothing_partial` (uses the existing `TestStore` fail-injection seam) |
| 16 | Idempotency across restart | R/G — `replaying_the_same_new_channel_event_after_restart_is_a_no_op`. Genuine red captured: temporarily bypassing the pre-decision `cycle_exists` check reproduced exactly the failure mode live-review F3 described (`UNIQUE constraint failed: rust_fee_cycles.cycle_id`, surfaced as a false "persistence failure" rather than a clean duplicate) — see "F3" below for the mechanism |
| 17 | Backpressure | R/G — `new_channel_dropped_under_backpressure_has_zero_effect` |
| 18 | Shadow transport tripwire | Covered structurally by `action_surface.rs`'s two guards (zero live capability construction anywhere reachable from this path) rather than a dedicated A3 fake-RPC test; not separately re-asserted with an explicit "zero mutation-call count" instrumented fake |

Plus, beyond the numbered list, from the live-review corrections:

- **Coalesced is zero-effect** (F2): `coalesced_new_channel_event_has_zero_additional_effect` — a second event for the same channel while the first is still the pending trigger-queue entry produces zero additional generation bump / action, but its own receipt.
- **Refusal is durable** (F1): `new_channel_refusal_is_durable_with_zero_effect`.

### §4.4 MANDATORY end-to-end reversion tripwire — implemented

`new_channel_end_to_end_commits_atomically_and_survives_restart` drives a
real `PreparedInitialFee` (DYNAMIC, known gossip prior) through
`CycleOwner::handle_new_channel` end to end and, after reopening the
Rust-owned store fresh, asserts: generation == 1; the persisted
`v2_state_json` contains the seeded `prior_mean_fee: 300`; exactly one
prepared `rust_fee_requests` row whose `message` contains the scripted fee;
one `rust_fee_trigger_events` receipt. Zero live mutation is structurally
guaranteed (no `ClnRpc`/broadcaster type is reachable from this path — see
`action_surface.rs`).

**Mutation demonstration** (required — "a receipt assertion alone is
explicitly vacuous"): `reversion_tripwire_mutation_demonstration_receipt_only_is_caught`
directly commits a receipt-only `FeeCycleCommit` (the exact shape a
reverted "log and return" owner handler would produce) and shows the SAME
assertions the real test makes (`request_count == 1`,
`snapshot.rows.len() == 1`) would FAIL against it (asserted here as `== 0`
against the receipt-only double, with an explanatory message pointing at
which real-test assertion would have caught the regression).

### §4.5 pre-decision epoch and T8b guards

- The two existing guards
  (`decision_gate_uses_pre_decision_epoch_not_fresh_flush`,
  `observation_cursor_uses_pre_decision_epoch`, both in
  `crates/revops-fees/tests/cycle.rs`) are confirmed **byte-identical**:
  `git diff df7f50b -- crates/revops-fees/tests/cycle.rs` touches exactly 5
  lines (the new `thompson_prior_std_fee` field in an unrelated `parse_cfg`
  helper and one new assertion in `default_cfg_matches_python_config_defaults`),
  neither guard function's body is in the diff. A byte-for-byte comparison
  of both function bodies (old blob vs. current, at their respective line
  ranges) is `IDENTICAL`. Both pass:
  `cargo test -p revops-fees --test cycle -- decision_gate_uses_pre_decision_epoch_not_fresh_flush observation_cursor_uses_pre_decision_epoch` → `2 passed`.
- A3-specific epoch guard: `new_channel_never_rewrites_an_existing_channels_skip_gate_epoch`
  seeds an unrelated existing channel's `skip_gate_prev`/`skip_gate_seen`,
  runs `handle_new_channel` for a DIFFERENT channel, and asserts the
  existing channel's epoch memory is byte-identical afterward, that the new
  channel is NOT added to `skip_gate_prev`/`skip_gate_seen` (that bootstrap
  classification stays exclusively `hydrate_from_strategy_rows`'s job — a
  first-appearance row is not falsely labeled comparable by this
  out-of-cycle path), and that the new channel's `cycle_state.last_update`
  DOES get the event-time epoch via the atomic commit's sync fields.

## Red-first evidence transcripts actually captured

1. **`notify::new_channel_signal`** (stub returning `None` always):
   ```
   test notify::tests::new_channel_signal_accepts_channel_id_only ... FAILED
     panicked: channel_id alone is enough evidence
   test notify::tests::new_channel_signal_fires_for_every_opening_state ... FAILED
     panicked: DUALOPEND_AWAITING_LOCKIN -> CHANNELD_NORMAL must signal
   test result: FAILED. 2 passed; 2 failed
   ```
   → green: `30 passed; 0 failed` (full `notify::` module).

2. **`fee_evidence::resolve_new_channel`** (stub returning `NotFound`
   always):
   ```
   resolve_new_channel_exactly_one_normal_fallback ... FAILED (expected exactly-one fallback, got NotFound)
   resolve_new_channel_funding_channel_id_match ... FAILED (expected funding channel_id match, got NotFound)
   resolve_new_channel_exact_scid_match ... FAILED (expected exact match, got NotFound)
   resolve_new_channel_multiple_normal_is_ambiguous ... FAILED (left: NotFound, right: Ambiguous)
   test result: FAILED. 3 passed; 4 failed
   ```
   → green: `7 passed; 0 failed`.

3. **F3 idempotency check bypassed** (`store.cycle_exists(...)` replaced
   with a hardcoded `Ok(false)`):
   ```
   revops: A3 NEW-CHANNEL COMMIT FAILED (failure #1): insert rust_fee_cycles row:
     UNIQUE constraint failed: rust_fee_cycles.cycle_id: Error code 1555: A PRIMARY
     KEY constraint failed; NO state was installed, NO action is authoritative...
   thread '...replaying_the_same_new_channel_event_after_restart_is_a_no_op' panicked:
     assertion `left == right` failed: a duplicate replay is NOT a persistence failure
     left: 1
     right: 0
   ```
   → green after restoring the real check: full suite green (below). This
   also incidentally proves the `cycle_id` PRIMARY KEY is a working
   transactional backstop even if the advisory pre-check were ever bypassed
   by a bug — a duplicate still cannot silently commit twice, it just
   surfaces as a (safe, non-duplicating) persistence failure instead of a
   clean "duplicate" outcome.

Every other new production increment (§4.2's precedence tests, the atomic
commit tests, F1/F2/F4/F6 fixes) was implemented and verified green without
a separately captured red transcript, for the time-pressure reasons stated
above. This is the honest state, not a claim of full red-first compliance
for those items.

## Live-review findings (F1–F6) — final disposition

- **F1 (durable refusal)** — RESOLVED, with a caveat. `prepare_new_channel`
  now always returns `NewChannelPreparation::{Ready, Refused}` and both are
  sent to the owner; `handle_new_channel_refused` offers to the SAME
  trigger discipline and writes a durable receipt via
  `record_trigger_receipt`. Caveat: this receipt is written through the
  EXISTING non-atomic path (the same one Dropped/Coalesced use), not
  wrapped in a full atomic commit with its own outcome row the way a Ready
  decision is — there is genuinely no state/action to be atomic WITH on a
  refusal, so the practical risk is low, but it is not byte-identical
  machinery to the Ready path. Test: `new_channel_refusal_is_durable_with_zero_effect`.
- **F2 (Coalesced zero-effect)** — RESOLVED. Only `TriggerOutcome::Enqueued`
  reaches decision; both `Dropped` and `Coalesced` return before any
  RNG/state/action work, each with its own receipt. Test:
  `coalesced_new_channel_event_has_zero_additional_effect`.
- **F3 (cross-restart event identity)** — RESOLVED. `PreparedInitialFee::
  event_key` is derived ONLY from resolved channel id + old_state +
  new_state + event timestamp (`new_channel_event_key`, `fee_scheduler.rs`)
  — never PID, never wall-clock-at-processing. Used both as an explicit
  pre-decision idempotency check (`store.cycle_exists`, a new
  `RunwayStateStore` trait method backed by a new `rust_fee_cycles`
  existence query) and as the atomic commit's `cycle_id` itself (a
  `PRIMARY KEY`, so even a raced duplicate is rejected transactionally,
  not just by the advisory check). Test:
  `replaying_the_same_new_channel_event_after_restart_is_a_no_op` (real
  restart: closes and reopens the store, constructs a fresh `CycleOwner`).
- **F4 (misleading shadow reason/disposition)** — RESOLVED. See §3.3 above.
- **F5 (owner blocks on the store actor)** — **NOT RESOLVED.**
  `handle_new_channel` (and the new F3 idempotency check) call
  `RunwayStateStore` methods that are, for the production `ObserverHandle`
  impl, `blocking_send`/`blocking_recv` against the actor's channel — a
  genuine unbounded wait on the owner's OWN dedicated thread, exactly what
  contract §3.1 forbids. This is NOT new to this task: it is the SAME
  pattern the existing SeedOnce per-cycle commit path
  (`CycleOwner::run_cycle`, pre-existing code) already uses, and my A3 path
  inherited it directly rather than diverging.
  I judged that properly fixing this — restructuring so the atomic commit
  is dispatched without the owner blocking, installing state only from a
  commit-result message bound to the exact generation/event, while still
  preventing a same-channel event from racing ahead during the pending
  commit — is a genuine architectural redesign (an async actor-with-reply
  pattern, analogous to `fee_execution.rs`'s `budgeted()` helper but for a
  result that must eventually install owner state) that I could not safely
  design, implement, and test correctly in the time remaining without a
  material risk of introducing a subtler bug than the one being fixed
  (e.g., two commits racing to decide the same channel while one is
  in-flight). This is reported honestly as incomplete rather than rushed.
- **F6 (no-store / config-read-failure red refusals)** — PARTIALLY
  RESOLVED. The no-store branch was moved to fire BEFORE decision/RNG and
  now increments `persistence_failures` with a durable receipt (previously
  it fired after a wasted decision and was silent). The idempotency-check
  failure path added for F3 follows the same fail-closed, red, durable
  pattern. However, `resolve_fee_cfg` itself remains the SAME
  non-fallible, per-field-default-on-read-error surface used by EVERY
  fee-cycle path in this codebase (not A3-specific) — a genuine
  config-field read failure inside it still silently defaults rather than
  refusing. The one form of "config/DB unreachable" A3 DOES catch is a
  fully unreadable production DB file, because `resolve_peer_policy_async`
  (which opens the SAME DB file) runs and would refuse BEFORE
  `resolve_fee_cfg` is ever called. A full fix would mean giving
  `resolve_fee_cfg` a fallible signature workspace-wide, which I judged out
  of safe scope for this session (large blast radius across every existing
  caller and test).

## Non-goals / safety rails — compliance check

- No production `revenue_ops.db` writes: confirmed — every A3 write goes
  through `RunwayStateStore`/`ObserverHandle` (the Rust-owned observer DB);
  reads from the production DB use `revops_db::open_read_only` /
  `prefetch_rpc`, never a write handle.
- No new `setchannel` call site, no live capability construction: enforced
  by `action_surface.rs`'s two guards, unmodified in content, both pass.
- No sampling on the async callback thread: `prepare_new_channel` never
  touches `PyRandom`/`DecisionEntropy`; `decide_initial_fee` (which does)
  is only ever called from `CycleOwner::handle_new_channel`, on the owner
  thread, with `&mut self.rng` — the ONE long-lived stream.
- No RPC/DB wait *while holding the single-owner state boundary* — this is
  exactly where F5 remains unresolved; documented above.
- No use of drain time for nudge/epoch semantics: `dynamic_initial_fee`'s
  `record_posterior_nudge` and the post-broadcast state sync both use
  `prepared.event_ts` (the notification's own clock read, threaded through
  from `notify::new_channel_signal`'s `now` parameter), never
  `crate::now_unix()` read at dispatch time.
- No success summary for receipt-only/persistence-failed/governor-denied
  outcomes: `InitialFeeOutcome` is a typed enum every receipt/detail string
  switches on explicitly; F4's fix additionally guarantees the text can
  never read as an applied broadcast.
- A1/A2 unmodified: `apply_failure_nudge`/`handle_failed_forward`'s
  effect-before-offer ordering (the pre-audit hazard's point 3) was
  identified and deliberately NOT copied, and deliberately NOT "fixed" in
  place, per contract §6.

## Gate results (this checkpoint)

- `cargo test -p revops` — pass (part of full workspace run below).
- `cargo test -p revops-db` — pass (part of full workspace run below).
- `cargo test --workspace` — **2234 passed, 0 failed**, 0 unexpected
  ignored, across every crate/binary/integration-test target.
- `cargo fmt --all -- --check` — clean (exit 0).
- `cargo clippy --workspace --all-targets -- -D warnings` — clean (exit 0).
  Two real lints were found and fixed during this checkpoint: a
  `large_enum_variant` on `ChannelResolution`/`NewChannelPreparation`
  (fixed by boxing the large variants) and a `clone_on_copy` on a `Copy`
  test fixture.
- `git diff --check` — clean (no whitespace errors).
- Working tree: 15 modified files, 0 untracked files introduced, exactly
  the files listed under "Files touched" below.

## Files touched (final)

- `crates/revops/src/notify.rs` — pure parser (`new_channel_signal`).
- `crates/revops/src/fee_evidence.rs` — channel resolution, gossip
  filtering, out-of-cycle policy read (async prep half).
- `crates/revops/src/fee_scheduler.rs` — `PreparedInitialFee`,
  `NewChannelPreparation`, `decide_initial_fee` (+ helpers),
  `prepare_new_channel`, `CycleMsg::NewChannel`, `CycleOwner::
  handle_new_channel` / `handle_new_channel_refused`, `new_channel_event_key`.
- `crates/revops/src/fee_triggers.rs` — `FeeTrigger::NewChannel` variant.
- `crates/revops/src/fee_state.rs` — `RunwayStateStore::cycle_exists`.
- `crates/revops/src/fee_config.rs` — `thompson_prior_std_fee` resolution.
- `crates/revops/src/main.rs` — subscription wiring, two new `State`
  fields (`socket_path`, `production_db_path`).
- `crates/revops-fees/src/cycle.rs` — `FeeCfgSnapshot::thompson_prior_std_fee`.
- `crates/revops-fees/src/replay.rs` — optional decode of the new config
  field (oracle-fixture compatibility).
- `crates/revops-db/src/fee_runway.rs` — `FeeCycleCommit::trigger_receipt`
  (atomic receipt), `cycle_exists` query.
- `crates/revops-db/src/owner.rs` — `Command::CycleExists` +
  `cycle_exists`/`blocking_cycle_exists`.
- `crates/revops-fees/tests/cycle.rs`, `crates/revops-db/tests/notifications.rs`,
  `crates/revops-db/tests/owner.rs` — mechanical fixture updates for the
  new `FeeCycleCommit`/`FeeCfgSnapshot` fields (no guard-function bodies
  touched).
- `crates/revops/tests/fee_scheduler.rs` — the owner-level A3 integration
  tests (§4.3/§4.4/§4.5 + F1–F4 corrections).

Not created: no new production files beyond the "likely" list in contract
§5 except `fee_evidence.rs` (justified above as reuse-over-duplication) —
no new `crates/revops/tests/fee_state.rs` or `fee_execution.rs` A3 tests
were added (time did not allow).

## Final state

No commit was created. Given F5 remains architecturally unresolved and F6's
deeper half is a documented, not a closed, gap, I judged that committing
this as a claimed-complete checkpoint would misrepresent its state. The
working tree is clean-editable (all gates above pass on the actual diff),
uncommitted, on branch `worktree-agent-a4963eebb9ce0b29b` at parent
`df7f50bbbaa45e1b4c063129c8d2d784ad31598c`. If the operator wants a WIP
commit preserved as-is (fully gate-green, but with F5/F6-deep open), that
is a one-command follow-up; I did not take that step unilaterally given the
task's explicit "ONE logical green commit" framing implies completeness.


---

# Recovery round 2 — F5 completed per the binding contract, plus F7/F8

Executed directly by the rust owner (no subagents) atop WIP `2338c93`, per
the operator's binding direction: "F5 may NOT be narrowed to a bounded
synchronous wait... off-owner commit dispatch + a generation/event-bound
commit-result message; install staged state only on success, keep
same-channel pending/race behavior fail-closed, and prove owner
responsiveness with a stalled store. Then handle A3-strict config refusal."
Two further supervisor findings (F7 state-generation binding, F8 config
freshness) were raised mid-round and are closed here too.

## F5 — the owner never blocks on a store reply (CLOSED)

Architecture: a two-phase pending state machine on the owner.
`handle_new_channel` = offer -> same-channel fail-closed guard -> no-store /
no-self-sender refusals -> park the frozen preparation in
`pending_initial_fees` -> `dispatch_cycle_exists_with_generation`
OFF-owner. The answer returns as `CycleMsg::InitialFeeStoreResult` on the
owner's own queue; only then does the decision/RNG run (still on the owner,
against CURRENT owner state), followed by `dispatch_commit_fee_cycle_guarded`
off-owner and an identity-bound commit-result message. Staged clones
install ONLY on a matched success. ALL A3 store interactions — including
refusal/dropped/coalesced/duplicate receipts (`dispatch_a3_receipt`) — are
off-owner; `RunwayStateStore` gained the three `dispatch_*` methods
(`ObserverHandle` impl: clone-the-handle + short-lived thread + blocking
call THERE; spawn failure still delivers an Err through the callback).

Red-first evidence (all observed FAILED before the fix, green after):
- `a_wedged_store_never_wedges_the_owner_thread` — the stalled-store proof
  through the REAL spawned loop (`spawn_with_thread_spawner` + a
  `WedgedStore` whose every blocking call parks forever and whose
  dispatches never deliver): RED = Query timeout with the pre-fix blocking
  implementation (observed `Timeout` at 5s); GREEN = Query round-trips
  while a NewChannel event is wedged in flight.
- `same_channel_event_while_commit_in_flight_is_refused_fail_closed` — RED
  observed (racing occurrence re-decided: 2 generations/2 actions, real
  commit-result orphaned into a conflict); GREEN = typed durable
  "REFUSED ... in flight" receipt, one commit, zero conflicts.
- `mismatched_idempotency_result_is_a_conflict_not_a_decision` and
  `mismatched_commit_result_is_a_conflict_not_an_install_or_discard` — RED
  observed against variant-only matching (forged results decided /
  discarded staged state and counted a false persistence failure); GREEN =
  exact event_key AND dispatch-generation equality required, mismatches are
  counted red conflicts (`initial_fee_conflicts`), the pending entry
  survives untouched, and the genuine result still completes.

## F6 — A3-strict config refusal (CLOSED)

`fee_config::resolve_fee_cfg_observed` threads an `AtomicU64` through
`db_layer` counting layer-(a) override QUERY failures (a legitimately
absent override row is NOT counted). `prepare_new_channel` refuses
(`NewChannelPreparation::Refused`, durable receipt path) when the count is
non-zero; shared `resolve_fee_cfg` / `resolve_neighbor_median_min_competitors`
keep their log-and-default posture byte-for-byte (they delegate/ignore the
counter). Red-first:
`config_query_failure_refuses_new_channel_preparation_instead_of_defaulting`
— full `prepare_new_channel` over a mock CLN socket + a config DB whose
`config_overrides` table is missing: RED observed = `Ready` with struct
defaults (max_fee_ppm=2000); GREEN = Refused naming the config failure.

## F7 — state-generation binding + Python-parity sequencing (CLOSED)

Store layer: `fee_runway::cycle_exists_with_generation` (idempotency answer
+ current generation in one read transaction) and
`fee_runway::commit_fee_cycle_guarded` (compare-and-set inside the same
`BEGIN IMMEDIATE`: generation mismatch = in-band `GenerationConflict`,
NOTHING written), plus actor commands and `ObserverHandle` blocking/
dispatch wrappers. Owner: tracks `state_generation` (set at SeedOnce
hydration, scheduled-commit success, A3 install; adopted from the store on
first contact and on conflicts), binds each A3 commit to the exact
generation the decision was computed against
(`Committing.expected_prior_generation`), and installs ONLY when the owner
has not advanced past that basis and the committed generation is exactly
basis+1. Sequencing (supervisor refinement): `run_or_defer_cycle` DEFERS a
`RunPrepared` cycle while any A3 store result is pending (bounded to ONE
slot — a newer prepared snapshot supersedes the older deferred one loudly,
counted in `deferred_cycles_superseded`); the deferred cycle runs from the
result handler the moment the pending map clears, so the next cycle always
consumes the synchronized post-A3 state (Python's `_state_lock`
serialization, without blocking the owner).

Evidence:
- `run_prepared_during_inflight_a3_commit_is_deferred_until_the_install` —
  RED observed against the pass-through production entry (cycle ran
  immediately, committing a pre-A3 epoch); GREEN = deferred, then runs
  post-install and its flush carries the A3-seeded nudge
  (`posterior_bias [[300.0, 0.3,`), zero conflicts, memory == DB.
- `deferred_cycles_are_bounded_and_superseded_loudly` — RED observed
  (both cycles ran); GREEN = one supersede counted, exactly one deferred
  cycle runs.
- `a3_commit_against_an_advanced_store_is_a_conflict_not_a_stale_write` —
  written after the CAS landed (the message-shape rework forced the
  implementation order), therefore MUTATION-VERIFIED: neutralizing the CAS
  comparison (`&& false`) reds it (stale commit landed as generation 3);
  restored sha256-exact.
- `late_a3_callback_after_owner_advance_never_installs_stale_state` —
  same post-hoc status, MUTATION-VERIFIED: forcing `owner_unadvanced =
  true` reds it (stale install, zero conflicts); restored sha256-exact.
  Honest note: these two tests were green-on-arrival because the fix
  preceded them; the mutations above are the discriminating evidence.

## F8 — A3 config freshness (CLOSED)

`prepare_new_channel` now takes the shared `PythonOptionCache` and, BEFORE
freezing any evidence, performs a fresh `listconfigs` refresh off-owner. A3
is strict: refresh failure REFUSES (typed, durable-receipt path) even
though a stale cached snapshot exists; the scheduled-cycle path keeps its
keep-last-good posture untouched. The signature refactor landed first as a
verbatim behavior port (F6 test stayed green), then red-first:
- `a3_preparation_uses_a_fresh_listconfigs_value_not_the_stale_cache` —
  RED observed (decided on the stale cached 555, not the live 777);
  GREEN = fresh value reaches `cfg.max_fee_ppm`.
- `a3_preparation_refuses_when_the_listconfigs_refresh_fails` — RED
  observed (`Ready` on the stale cache during a listconfigs outage);
  GREEN = Refused naming the failed refresh.

## Owner verification round 2 (mine, this round)

- §4.4 reversion mutation re-run against the NEW architecture: a
  recording-only regression (receipt dispatched, decision skipped, in the
  idempotency continuation) reds
  `new_channel_end_to_end_commits_atomically_and_survives_restart`
  (generation 0 vs 1). Restored sha256-exact.
- CAS-bypass and install-rule mutations: see F7 above, both red, both
  restored sha256-exact (verified via a recorded baseline manifest).
- T8b guards: `decision_gate_uses_pre_decision_epoch_not_fresh_flush` and
  `observation_cursor_uses_pre_decision_epoch` byte-identical to
  `df7f50b` (per-function sha256 equality) and green. The only cycle.rs
  delta vs df7f50b is WIP round 1's `thompson_prior_std_fee` cfg-parse +
  default assertion (an A3 config addition, not a guard edit).
- `new_channel_event_key` remains free of wall-clock/pid inputs (grep).
- `old_fee_ppm_comes_from_channel_info_not_persisted_state` present and
  green (old_fee always from `channel_info.fee_proportional_millionths`).
- Durable-refusal-across-reopen, offer-first Dropped AND Coalesced
  zero-effect, atomic-failure injection: all green in the suite.
- `tests/action_surface.rs` green (3/3) — the A3 path remains structurally
  broadcast-free; shadow mode records, never mutates.

## Gate results (recovery round 2 checkpoint)

- `cargo test --workspace`: **2245 passed / 0 failed**
- `cargo clippy --workspace --all-targets -- -D warnings`: clean
- `cargo fmt --all --check`: clean
- `git diff --check`: clean
- Files touched this round: `crates/revops-db/src/{fee_runway,owner}.rs`,
  `crates/revops/src/{fee_scheduler,fee_state,fee_config,main}.rs`,
  `crates/revops/tests/fee_scheduler.rs`.

## Known accepted behaviors (disclosed)

- A racing same-channel A3 occurrence is refused fail-closed (durable
  typed receipt); the event is NOT replayed internally — CLN's own
  redelivery/restart replay (stable event_key) is the recovery path.
- An A3 commit orphaned by a genuine external store advance (CAS conflict)
  leaves the recorded cycle row as evidence and discards the staged
  in-memory state; the loss is loud and counted, never silent — Python's
  in-lock handler cannot lose the nudge this way, but Python also blocks
  its whole plugin on the lock; the deferral rule removes the only
  in-process schedule that could trigger this.
- The scheduled SeedOnce commit remains unguarded (sole-writer owner
  performs it synchronously; guarding it would abort whole multi-channel
  cycles over an A3-only race that the deferral rule already prevents).
