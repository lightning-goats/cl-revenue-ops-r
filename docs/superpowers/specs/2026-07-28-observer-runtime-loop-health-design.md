# Observer Runtime and Durable Loop Health Design

**Status:** Task 57 correction design. The original checkpoint was approved;
the 2026-07-28 implementation review correction contract amends it with the
durable suspension, bounded scheduler-ingress, structural construction, and
post-start activation rules below.

## Goal

Provide one fail-closed runtime and health foundation for the whole-plugin Rust
port without claiming that unimplemented subsystem loops already work. This
checkpoint may production-spawn only the real fee observer pass. Rebalance,
planner, LN+, and Boltz are enumerated as required identities and exercised by
fake passes, but remain durably `not_wired` until their real adapters land in
the later subsystem tasks.

## 2026-07-28 correction amendment

The implementation review found eight gaps. This amendment is authoritative
where it differs from the original checkpoint text below.

- Durable health stores `runtime_status` (`active` or `suspended`),
  `last_suspended_at`, and a bounded `last_suspension_reason`. Suspension has
  precedence over terminal pass or error. Terminal writes never change runtime
  status, so a pass finishing after concurrent suspension cannot mask it. Every
  suspension cause stops local admission and retries the marker until it commits
  or the observer-store actor is provably closed.
- Registration records exact current-boot wiring. It may downgrade historical
  `ready` to current `not_wired`; this supersedes sticky-ready because wiring is
  current reachability while pass, error, and suspension fields are retained
  historical evidence. Registering current `ready` explicitly reactivates the
  loop without erasing prior suspension or error history.
- The canonical unshipped schema includes terminal generation/status and runtime
  suspension fields from creation. Partial legacy tables are rejected; startup
  never ALTER-defaults missing terminal evidence and then invents interruption.
  Finish and fail are single-terminal CAS operations requiring
  `terminal_generation < generation`.
- `ObserverRuntime::start` requires a non-forgeable `ObserverModeToken` issued
  only from a validated passive or autonomous-shadow mode. Its public
  `ObserverPassSet` has private fields and accepts only concrete vetted observer
  pass types; arbitrary fake pass maps are private test plumbing. Validated mode
  selection invokes a live action factory only for `LiveAuthority`, pinned by a
  compile-fail proof and observer-mode panic-factory tests.
- Every fee-owner producer shares one fixed-capacity Tokio MPSC ingress. Async
  notification, RPC, prefetch, and cadence producers await `send`; the plain
  owner thread uses `blocking_recv`; A3 callback threads use `blocking_send`.
  The wake path is bounded and private. Closed-owner RPCs return explicit errors,
  and notification or A3 work never silently drops.
- Deferred ACKs are total: an older deferred cycle receives explicit `Err` when
  superseded, the newest remains pending to the real terminal outcome, skip/error
  is never success, and owner loss closes every queued or held receiver. A3
  callbacks and cycle messages retain FIFO order on the same ingress.
- Autonomous cadence is constructed idle and activated only after
  `configured.start(state).await` succeeds. Startup failure drops the activation
  without issuing a request.

## Non-negotiable invariants

1. Python remains the sole mutation authority; Task 57 performs no live calls,
   deployment, arm creation/consumption, or authority transition.
2. Observer construction cannot accept, contain, or construct an action
   adapter. Default-safe enums are insufficient; the type graph must separate
   observer and live capabilities.
3. A pass is never reported successful because work was queued or dispatched.
   Success requires acknowledgement from the actual owner after its real
   outcome is known.
4. No pass executes until its durable begin record commits. Completion/error
   writes use generation compare-and-set so stale callbacks cannot overwrite a
   newer pass.
5. Missing persistence, an unrecordable completion, panic, queue overflow, or
   stale generation is visible and fail-closed. None becomes a clean pass.
6. Unwired subsystem identities never receive success-shaped no-op passes and
   are not counted reachable/effective in the parity checklist.

## Runtime identities and authority split

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LoopId { Fee, Rebalance, Planner, LnPlus, Boltz }

pub const REQUIRED_LOOPS: [LoopId; 5] = [
    LoopId::Fee,
    LoopId::Rebalance,
    LoopId::Planner,
    LoopId::LnPlus,
    LoopId::Boltz,
];

pub enum AuthorityRuntime {
    Observer(ObserverRuntime),
    Live(LiveRuntime),
}

pub struct ObserverRuntime {
    health: LoopHealthStore,
    fee: Option<LoopHandle>,
    // Later real observer handles; absence is explicit, never a no-op pass.
    rebalance: Option<LoopHandle>,
    planner: Option<LoopHandle>,
    lnplus: Option<LoopHandle>,
    boltz: Option<LoopHandle>,
}

pub struct LiveRuntime {
    fee_broadcaster: ClnFeeBroadcaster,
    // Later action adapters are added only here behind whole-plugin authority.
}
```

`ObserverRuntime::start` accepts a non-forgeable observer-mode token, Rust-owned
health store, and a private-field pass set whose public constructor accepts only
the concrete vetted fee observer pass in this checkpoint. It has no
live-capability or action-adapter parameter. `State` stores `AuthorityRuntime`;
it no longer stores an optional live broadcaster beside observer fields.

## Durable schema and store contract

The table belongs in the Rust-owned observer database and is initialized with
the existing WAL schema. The Python production database remains read-only.

```sql
CREATE TABLE IF NOT EXISTS rust_loop_health (
    loop_name TEXT PRIMARY KEY,
    wiring_status TEXT NOT NULL
        CHECK (wiring_status IN ('not_wired', 'ready')),
    generation INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0),
    terminal_generation INTEGER NOT NULL DEFAULT 0 CHECK (terminal_generation >= 0),
    terminal_status TEXT NOT NULL DEFAULT 'none',
    runtime_status TEXT NOT NULL DEFAULT 'active',
    last_suspended_at INTEGER,
    last_suspension_reason TEXT,
    last_started_at INTEGER,
    last_passed_at INTEGER,
    last_error_at INTEGER,
    last_error TEXT,
    coalesced_total INTEGER NOT NULL DEFAULT 0,
    dropped_total INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL
);
```

Store operations are typed commands on the existing observer DB actor:

```rust
register_loop(loop_id, wiring_status, now)
begin_loop_pass(loop_id, now) -> generation
finish_loop_pass(loop_id, generation, completed_at)
fail_loop_pass(loop_id, generation, failed_at, bounded_error)
increment_loop_backpressure(loop_id, coalesced_delta, dropped_delta, now)
suspend_loop(loop_id, suspended_at, bounded_reason)
list_loop_health() -> Vec<LoopHealthRow>
```

Rules:

- Registration is idempotent and records exact current-boot wiring; historical
  terminal, error, and suspension evidence is retained.
- Begin is refused for `not_wired` or durable `suspended`; for ready/active it
  increments generation and sets `last_started_at` transactionally.
- Finish/fail update with matching generation and only while
  `terminal_generation < generation`; zero changed rows is a stale or duplicate
  terminal-generation error.
- A pass preserves previous error and suspension history. Current health is
  derived from wiring, suspension precedence, generation, and terminal kind,
  not timestamp ordering or erased history.
- Error text is bounded before persistence.
- Startup reconciles only `generation > terminal_generation` and records
  `previous_generation_incomplete_on_restart` before new work.
- If any lifecycle or backpressure write cannot be recorded, that owner suspends
  new passes and durably marks suspension before returning; an unmatched begin
  remains visible.

## Bounded single-flight owner

Every loop owner has one in-flight pass and at most eight pending distinct
keys. Ingress is bounded before the owner, unlike the current fee scheduler's
unbounded MPSC channel.

`request(key)` returns exactly one of:

- `Enqueued`: a new key was admitted;
- `Coalesced`: the same key is already running/pending and its counter was
  durably incremented;
- `Dropped`: capacity is full or the owner is suspended and the drop counter
  was durably incremented.

The owner uses a mutex-protected ingress state plus a notification primitive,
not an unbounded channel. It durably begins, executes the injected
`ObserverPass` in a child Tokio task, catches join panic/error, then durably
finishes or fails with the matching generation. Only after the terminal write
commits may the next key run.

The framework is fully testable with fake `ObserverPass` implementations for
all five identities. Production construction in this checkpoint supplies a
real fee pass only; the other four rows stay `not_wired` and have no handle.

The fee scheduler inner owner ingress is also fixed-capacity. Async notification,
RPC, prefetch, and cadence producers await a bounded Tokio MPSC send; the plain
owner thread uses `blocking_recv`; dedicated A3 callback threads use
`blocking_send`. The separate wake path is bounded and private. No public
unbounded sender remains.

## Fee completion acknowledgement

The existing fee path must not translate `send(CycleMsg::RunPrepared)` into a
pass. `RunPrepared` carries a completion sender, and `CycleOwner` replies only
after the real cycle outcome and required state write are known. Prefetch,
send, owner, persistence, panic, and disconnected-reply failures all become a
durable loop error. Deferred or superseded work must resolve its acknowledgement explicitly rather
than dropping it. An older deferred cycle receives `Err`; the newest remains
pending until its real terminal outcome; skip/error is never success; owner loss
closes all queued or held receivers.

Fee-trigger ingress is migrated behind the bounded owner seam or receives an
equivalent bounded front door; the current inner `TriggerQueue` does not bound
messages before they reach the owner. The autonomous cadence is constructed idle
and its activation handle is consumed only after `configured.start(state).await`
succeeds.

## Health surface

`revenue-health.loops` reads `list_loop_health()` and returns all five required
identities with wiring status and durable timestamps/counters. The `loops` gap
is removed only for this durable inventory. A `not_wired` row is an honest
state, not an error and not a successful pass. Database read failure is a
section-local in-band error, never a fabricated healthy loop set.

## RED-first proof matrix

1. Schema, registration, generation increment, generation-CAS stale refusal,
   error bounding, pass/error history, and restart-incomplete reconciliation.
2. Blocked fake pass proves maximum concurrency one.
3. Duplicate keys coalesce; nine distinct pending keys prove the ninth drops.
4. Begin-write failure prevents the pass from executing.
5. Error and panic are recorded; a later generation may recover only after the
   terminal write succeeded.
6. Unrecordable terminal outcome suspends the owner and leaves unmatched begin.
7. Exact five-identity registration; removal of any identity reds.
8. Fee dispatch without real owner acknowledgement cannot record pass.
9. Health RPC round-trips distinctive durable rows and preserves `not_wired`.
10. Compile-fail/action-surface/panic-factory proofs that observer construction
    cannot accept or construct `ClnFeeBroadcaster`, `PaymentMode::Live`, or
    LN+/Boltz `ExecutionMode::Armed`.
11. Mutation checks remove one begin, finish, fail, coalesced, and dropped
    persistence call in turn; the corresponding test must red before exact
    restoration.
12. Suspension-marker retry/recovery during in-flight and no-pass failures,
    reopen persistence, terminal-write non-erasure, and health precedence.
13. Scheduler-ingress saturation for async, callback, RPC, and cycle producers;
    FIFO callback/ACK ordering; closed-owner errors; no unbounded source seam.
14. Deferred ACK supersession, newest completion, skip/error, and owner-loss
    receiver closure.
15. Paused-time cadence construction proves zero work before activation.
16. Canonical-schema rejection and duplicate same-generation terminal refusal.

## Files

- Create `crates/revops-db/src/loop_health.rs`
- Modify `crates/revops-db/src/{lib,notifications,owner}.rs`
- Create `crates/revops/src/{runtime,loop_health}.rs`
- Modify `crates/revops/src/{lib,main,fee_scheduler,rpc_health}.rs`
- Create `crates/revops-db/tests/loop_health.rs`
- Create `crates/revops/tests/runtime.rs`
- Modify `crates/revops/tests/{manifest,action_surface}.rs`
- Modify `docs/port/PARITY-CHECKLIST.md`

## Deferred production reachability

Tasks 4–7 of the pre-cutover programme must instantiate real rebalance,
planner, LN+, and Boltz observer passes and then change those rows from
`not_wired` to `ready`. They must also seal their concrete action constructors
behind the whole-plugin live capability. Task 57 alone cannot claim those four
loops reachable or effective.
