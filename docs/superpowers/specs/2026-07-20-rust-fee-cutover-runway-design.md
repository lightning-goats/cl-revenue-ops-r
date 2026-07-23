# Rust Fee Cutover Runway Design

**Date:** 2026-07-20  
**Status:** Approved design  
**Target cutover:** 2026-08-03, America/Denver  
**Scope:** Rust fee authority preparation in `cl-revenue-ops-r`, the Python
fee-authority handoff in `cl_revenue_ops`, shadow operation on `lnnode`, and
user-level systemd automation owned by `lightningd`

## Context

Python is the sole live fee authority on `lnnode`. Rust is deployed beside it
with `observer=true` and `fee-dryrun=true`, reads the Python production
database through a read-only actor, and writes only its Rust-owned observer
database and dry-run artifacts.

The deployed Rust release at source commit
`7d8e79ec307fd10bd1a775a236148a642a0a506f` and binary SHA-256
`ff648376758b9a97de7642adbf1c258494744c54e33c31a712dcc8c742d1428c`
strictly replayed a natural six-cycle Python-authority window:

- 252 evaluated channels and terminal outcomes;
- 54 adjustments;
- zero replay mismatches;
- zero failed or dropped captures;
- configuration parity across 118 comparable keys;
- exact forward-ingestion parity over the selected seven-day window.

That evidence proves the selected decision window, not cutover readiness. The
deployed Rust release deliberately has no live broadcast path. Cutover also
still requires Rust-owned state continuity, mempool recording, production
trigger wiring, a live governor/ledger boundary, an unambiguous Python
fee-authority handoff, rehearsal, rollback, and sustained evidence.

The operator approved a two-week runway whose purpose is to activate every
safe observational component as soon as possible, collect rolled-up evidence,
find and fix bugs, and arrive at a manual fee-subsystem cutover with a frozen,
well-soaked candidate. The target date is a goal, not permission to waive a
gate.

Live deployment facts rechecked during design self-review on 2026-07-20:

- the `lightningd` systemd user manager is currently running, but
  `loginctl show-user lightningd` reports `Linger=no`;
- the filesystem holding `/data` and `/home/lightningd` is 96% used with
  approximately 4.3 GiB available;
- the host timezone is `America/Denver`.

Persistent user timers therefore require one-time lingering activation, and
capture/report retention must preserve a substantial free-space reserve.

## Goals

- Exercise the cutover scheduler, state lifecycle, evidence recorders,
  triggers, governor, ledger, and exact outbound request construction in live
  shadow mode without invoking a mutation RPC.
- Compile and fully test the real CLN `setchannel` adapter while making it
  impossible to select accidentally.
- Add a positive, queryable Python fee-authority handoff instead of inferring
  authority from plugin presence or timing.
- Gather ten-minute health evidence and one daily strict replay/parity rollup
  through `lightningd` user-level systemd units on `lnnode`.
- Reset promotion soak automatically after a candidate change or failed gate.
- Rehearse cutover and rollback against copied production state and a fake CLN
  RPC endpoint.
- Keep the actual cutover manual, release-bound, short-lived, and fail-closed.

## Non-goals

- Automatically fixing code, deploying releases, disabling Python authority,
  arming Rust, or performing cutover from a timer.
- Giving Rust authority over rebalancing, payments, channel opens/closes,
  Boltz, LN+, planner execution, or on-chain actions.
- Unloading the Python plugin as a whole. Only its fee mutation authority is
  handed off; unported Python subsystems remain loaded.
- Writing Rust shadow state into the Python production database.
- Treating a deadline, missing evidence, stale telemetry, or an inconclusive
  test as authorization to act.
- Introducing an external coordinator or duplicate operational source of
  truth.

## Safety invariants

1. Python remains the sole live fee authority throughout the shadow runway.
2. Rust shadow operation remains `observer=true` and `fee-dryrun=true`.
3. The production Python database remains read-only from Rust in shadow.
4. Shadow and live execution share request construction, but not executor
   capability. The shadow executor has no CLN mutation handle.
5. The real adapter is denied unless every independent cutover gate passes.
6. The timers never possess or create a valid cutover arm and never change
   fee authority.
7. The Python fee-authority switch defaults to enabled and preserves current
   behavior until explicitly disabled during a manual cutover.
8. Python's scheduled, triggered, and manual fee mutation paths all consult
   the same authority switch. Disabling authority for cutover uses CLN
   `setconfig` with `transient=false` so the handoff survives restart.
9. Rust verifies Python fee authority is disabled before every live broadcast
   batch. Missing, malformed, stale, or conflicting evidence denies the batch.
10. State-persistence failure, governor denial, ledger failure, ambiguous RPC
    outcome, or an unreconciled prior action denies further execution.
11. A candidate SHA change resets the clean-soak clock. No report can carry a
    green streak across binaries.
12. A failed red gate prevents promotion and makes the target cutover a
    `NO-GO`; the date slips rather than the gate.
13. No live test invokes a fee, payment, rebalance, channel, planner, Boltz,
    LN+, or on-chain mutation RPC.
14. Rollback artifacts are checksummed and verified before the corresponding
    deployment.

## Selected architecture

Use two node-local, user-level systemd timers under the `lightningd` account:

- a cheap ten-minute watcher for health, safety, liveness, and incremental
  evidence;
- a daily rollup for capture rotation, strict replay, parity gates, state and
  ledger checks, bounded evidence retention, and schedule advancement.

The timers invoke one versioned runway controller so gate definitions,
candidate identity, report schema, retention, and milestone logic have one
source of truth. Systemd supplies scheduling and process supervision; it does
not encode cutover policy.

Two alternatives are rejected:

- **One stateful timer for everything:** fewer units, but a long daily replay
  can obscure or delay the cheap liveness checks and makes failure diagnosis
  less clear.
- **Off-node orchestration:** keeps tooling away from the production host, but
  makes evidence continuity depend on the development host, SSH, and network
  availability. The bounded node-local runner is more reliable and does not
  need a Rust toolchain on `lnnode`.

## Fee execution architecture

### Shared request construction

The fee engine produces a typed `SetChannelFeeRequest` containing the exact
CLN request fields, including dynamic `htlcmax` when enabled. One serializer
and validator owns conversion into the `setchannel` RPC payload. Shadow and
live execution consume the same typed request so shadow evidence exercises the
real outbound shape.

Request construction remains pure. Invalid channel identifiers, invalid fee
ranges, money overflow, non-finite values, missing required evidence, and
unsupported field combinations fail before executor selection.

### Capability-separated executors

The existing pure executor evolves into two explicit implementations:

- `ShadowFeeExecutor` records the serialized request, governor result,
  expected state transition, and `would_broadcast` outcome in the Rust-owned
  database. It is constructed without a CLN mutation handle.
- `ClnFeeExecutor` owns the mutation-capable RPC interface and can invoke
  `setchannel` only after a `CutoverAuthorizer` issues a short-lived batch
  authorization.

The scheduler does not branch directly on booleans before each RPC. Startup
constructs exactly one executor after validating the complete operating mode.
An invalid or partial mode fails plugin initialization rather than silently
falling back to a mutation-capable executor.

### Cutover authorizer

Entering a live-authority process session requires all of the following at the same time:

- `observer=false`;
- `fee-dryrun=false`;
- an explicit fee-broadcast option enabled;
- a valid cutover arm;
- a positive Python RPC response that fee authority is disabled;
- fresh scheduler evidence and writable Rust state;
- production governor authorization and a healthy ledger;
- no unresolved or ambiguous prior execution.

The arm authorizes construction of one live-authority process session; it is
not a perpetual credential and is not polled from disk before every request.
The plugin validates and atomically consumes the arm while entering live mode.
Every broadcast batch then rechecks Python authority, scheduler evidence,
state, governor, ledger, and quarantine. A Rust process restart loses the
in-memory authority capability and requires a fresh arm.

The cutover arm is a mode-`0600` JSON object containing:

- schema name and version;
- node identity;
- subsystem name `fees`;
- Rust source commit;
- exact running-binary SHA-256;
- activation and expiry timestamps;
- a unique operator-generated nonce.

The running process hashes its own executable and verifies every arm field.
The arm is rejected if it is missing, malformed, for another node or
subsystem, not yet active, expired, or bound to another commit or binary.
Timers cannot generate, refresh, or install the arm.

### Ambiguous execution

An RPC transport failure after submission is not treated as a safe retry. The
request and uncertainty are persisted, the executor enters quarantine, and no
more broadcasts occur until read-side reconciliation proves the channel's
actual policy or an operator resolves the incident. Restart restores this
quarantine before the scheduler can acquire live authority.

## Python fee-authority handoff

The Python plugin gains one dynamic, default-true fee-authority control and a
read-only status surface. The control gates every Python path that can mutate
channel fees:

- the naturally scheduled fee loop;
- failed-forward, policy-change, wake, and other triggered evaluation paths;
- manual fee-cycle and direct fee-setting RPCs;
- dynamic `htlcmax` changes that share `setchannel`.

Disabling authority returns an explicit status and leaves analytics, capture,
reporting, rebalancing, planner recommendations, and other unported Python
subsystems loaded. The status response includes the effective authority
boolean, generation/epoch, transition timestamp, and a stable reason.

Rust queries that status immediately before each live broadcast batch and
binds the response epoch into its batch authorization. Authority becoming
enabled, the RPC disappearing, a stale epoch, or a malformed response aborts
the batch. This is a positive handoff; Rust never infers safety merely because
Python appears stopped or quiet.

Python deployment happens first with authority still enabled. The default-on
change must be behaviorally neutral and independently rollbackable. Shadow
tests of the switch use `setconfig ... transient=true` and immediately restore
authority. The manual cutover uses `transient=false` (CLN v26.06.1's default),
then verifies the returned `config.setconfig` source so Python cannot silently
reacquire authority after a restart.

## Stateful cutover shadow

### Dual evidence lanes

Two independent lanes run during the runway:

1. **Oracle lane:** naturally scheduled Python cycles are captured atomically
   and replayed strictly through the Rust kernel. This remains the exact
   parity gate.
2. **Autonomous lane:** the deployed Rust scheduler runs the cutover
   `SeedOnce` lifecycle, persists its own evolving state, records would-be
   broadcasts, and consumes production triggers without mutating CLN.

The autonomous lane must not replace strict replay. Its purpose is to expose
restart, accumulated-state, trigger, recorder, governor, and integration bugs
that an isolated replay cannot reveal. Reports label oracle mismatches and
autonomous divergence separately.

### Rust-owned persistence

The observer database gains versioned tables for:

- controller state and state generations;
- Rust mempool fee samples;
- shadow cycle summaries and terminal outcomes;
- attempted request/audit rows;
- trigger receipts and coalescing decisions;
- governor and dry-run ledger outcomes;
- execution quarantine and reconciliation state;
- timer snapshots and report identity.

Schema migration is idempotent and transactional. It never targets the Python
production database. A failed migration or state flush keeps live execution
disabled and turns the runway red.

### Mempool recorder and triggers

Rust records the same decision-relevant mempool sample cadence in its own
database and compares the resulting 24-hour moving average with Python during
shadow. Cutover reads only Rust-owned samples.

`forward_event`, failed-forward nudges, policy changes, wake-all, Vegas spike
checks, and the fixed-interval scheduler are wired through one bounded,
coalescing queue. Shadow mode records why each trigger did or did not produce a
cycle. Backpressure drops are explicit red evidence; they are never silently
treated as an empty workload.

## Timer and controller design

### User-level units

All units are installed for and run as `lightningd` through
`systemctl --user`. No root system unit or `lightningd` daemon restart is
required. Because `lightningd` currently has `Linger=no`, deployment has a
one-time prerequisite to enable lingering for that account and verify the user
manager survives logout. Enabling linger may require privileged operator
access, but the runway services and timers remain unprivileged user units.

The units are:

- `revops-shadow-watch.service` and `.timer`, every ten minutes;
- `revops-shadow-rollup.service` and `.timer`, once daily.

The services use a shared non-blocking lock, bounded runtime, atomic output
replacement, `NoNewPrivileges=true`, a private temporary directory, and
write access limited to the runway, capture, and Rust observer-data paths.
They call the local Lightning Unix socket and do not require SSH.

`Persistent=true` permits a missed run after a host restart to execute once,
but the controller records the evidence gap. It does not synthesize missing
green samples.

### Ten-minute watcher

The watcher records a compact JSON snapshot and exits nonzero on a hard safety
failure. It checks:

- Python and Rust plugin liveness;
- effective Python fee-authority state;
- Rust observer, dry-run, and broadcast configuration;
- source commit, installed binary hash, and candidate identity;
- scheduler heartbeat and most recent completed cycle;
- state-sink generation and error counters;
- trigger queue depth, coalescing, and drops;
- mempool recorder freshness;
- shadow governor and ledger health;
- execution quarantine state;
- Rust mutation-call count, which must remain zero in shadow;
- production database descriptors remaining read-only from Rust;
- disk usage and bounded-retention headroom.

The watcher also keeps authority captures below their existing 32-envelope
retention limit. Immediately after a capture reaches six complete natural
cycles, it closes and drains the run, freezes it atomically, and opens the next
run. It never triggers a fee cycle. Capture enable/disable calls use
`setconfig transient=true` so hourly rotation does not append durable config
entries. Rotation is expected roughly hourly at the current 600-second Python
fee interval; if the watcher is delayed, it rotates at the first safe
opportunity and marks any possible evidence gap. Reaching the retention limit
before a successful freeze is red.

Individual snapshots are append-only and small. Routine green samples are
rolled up rather than emitted as operator messages.

### Daily rollup

The daily service:

1. closes, drains, and freezes any partial active Python capture run;
2. immediately opens the next observational capture window;
3. validates the ordered set of frozen six-cycle and partial windows since
   the prior rollup;
4. strictly replays those frozen windows with the tested replay binary;
5. runs configuration and implemented read-RPC parity;
6. compares oracle outcomes with autonomous shadow decisions and state;
7. checks mempool moving-average parity, trigger coverage, state generations,
   governor decisions, ledger reconciliation, and restart markers;
8. aggregates ten-minute watcher results;
9. classifies gates as green, yellow, or red;
10. advances or resets the candidate clean-soak clock;
11. writes versioned JSON and Markdown reports plus an atomic `latest` link;
12. prunes evidence within the retention policy.

Capture rotation is observational and never invokes `revenue-fee-cycle`,
`revenue-wake-all`, `revenue-set-fee`, `setchannel`, or another action RPC.

### Reports and retention

Reports live under
`/home/lightningd/revops-r-deploy/runway/reports/` and include:

- report schema/version and interval;
- candidate source commit and binary SHA-256;
- unit and plugin health;
- exact replay counts and mismatch details;
- autonomous divergence and trigger coverage;
- state, mempool, governor, ledger, and quarantine health;
- zero-mutation proof;
- gate classification and clean-soak duration;
- evidence paths and checksums;
- next scheduled milestone and current go/no-go state.

Evidence retention has a 512-MiB hard ceiling and must preserve at least 3 GiB
of filesystem free space. Green envelopes are pruned oldest first after their
checksums and rollup have been published. Red windows and their minimal
reproducer are preferred within the same ceiling, but the controller never
exceeds either the byte cap or the free-space reserve. A retention failure or
insufficient safe headroom is red rather than an excuse to discard the active
failure window.

## Gate semantics

### Red gates

Any of the following resets promotion and prevents cutover:

- a Rust mutation RPC in shadow;
- simultaneous Python and Rust fee authority;
- observer/dry-run/broadcast configuration inconsistent with shadow;
- replay mismatch, incomplete transcript, failed capture, or dropped capture;
- corrupt, missing, or unflushable Rust state;
- trigger queue loss or an unhandled trigger path;
- stale/missing Rust mempool evidence needed for a decision;
- governor or ledger inconsistency;
- an ambiguous execution or active quarantine;
- candidate hash/provenance disagreement;
- production DB opened writable by Rust;
- a failed rehearsal safety injection;
- report tampering, non-atomic publication, or exhausted retention headroom;
- a capture reaching its retention limit before it is safely frozen;
- the `lightningd` user manager not being persistent while timers are expected
  to enforce the runway;
- a gate-starved stateful-shadow window: the dry-run decision surface must be
  live (see "Live decision-surface engagement gate" below) — a soak day whose
  non-sleeping rows are majority `waiting_window`, or whose would-broadcast
  count does not track Python's applied fee-change count, is red.

### Live decision-surface engagement gate (added 2026-07-23)

Shadow-data analysis of the 2026-07-22/23 window found the live dry-run
journal had **never exercised the fee engine**: 1442 of ~1925 non-sleeping
post-deploy rows (75%) carried trace `disposition: waiting_window`, the
dry-run proposed 1 adjustment while Python applied 247, and every one of
the sampling channel `958668x1046x0`'s 47 rows was gate-held. Strict replay
being byte-exact is NOT evidence against this — replay runs on Python's
captured pre-state; the live gate starvation is a shadow-mode input skew.

Root cause (hydration-epoch skew, the same class T8b fixed for skip-reason
classification but not for the decision path): `adjust_channel_fee`'s
observation-window check (`crates/revops-fees/src/cycle.rs:1622`) and its
`observation_cursor` for volume/forward reads consume the freshly
rehydrated `cycle.last_update` — Python's POST-decision flush value,
~50 s old when the flush-triggered Rust cycle runs, because Python's flush
advances `last_update` for every evaluated channel each cycle. Python's own
gate (py 5768-5782) reads its in-memory PRE-decision value (~one fee
interval old). Result: shadow Rust perpetually waits on channels Python is
actively evaluating, and the T8b classifier (correctly using the
pre-decision epoch) then mislabels the hold as `fee_unchanged`. Under
SeedOnce/authoritative lifecycle the two epochs coincide by construction,
so cutover would not inherit the starvation — but it WOULD inherit an
unmeasured fee engine: the soak would have validated the gate, not the
decisions.

Required fix before the stateful-shadow soak counts: extend the T8b
pre-decision epoch to the decision gate and observation cursor (a
decision-path fix — per "Candidate changes and bug fixes" this requires a
fresh 72-hour clean soak).

Gate measurement, per soak day, from the dry-run journal vs Python's
`fee_changes`:

- `waiting_window` share of non-sleeping rows below 20% (red above 50%,
  yellow between);
- Rust would-broadcast count within 0.5x–2.0x of Python's applied
  fee-change count over the same window (RNG divergence keeps individual
  decisions non-identical; the RATE must track);
- the flapper-class check: any channel Python adjusted in ≥ 5 consecutive
  cycles must show at least one non-`waiting_window` Rust evaluation in
  the same span.

### Yellow gates

Insufficient natural occurrences, a short transport outage, or a missed timer
sample is yellow only when safety evidence remains intact. Yellow never counts
as green soak and cannot be silently upgraded by elapsed time.

### Candidate changes and bug fixes

Any source commit or binary SHA change starts a new candidate and resets the
clean-soak clock. The timer automates detection, evidence preservation,
classification, regression reruns, and schedule status. It does not author a
patch or deploy a release.

Fixes follow test-driven development. An observational reporting-only fix
requires at least 24 clean hours. A decision, state, execution, authority, or
timer-gate fix requires a new 72-hour clean soak. The final release candidate
always requires 72 continuous green hours.

## Two-week schedule

| Window | Automated and engineering work | Promotion requirement |
| --- | --- | --- |
| Jul 20-22 | Implement with TDD; add the dormant live adapter, Python handoff, SeedOnce persistence, Rust mempool recorder, production triggers, governor/ledger wiring, controller, reports, and user units. | Full local suites pass; the action adapter is exercised only against fake RPC. |
| Jul 22-25 | Deploy the full shadow candidate; begin ten-minute watches and daily strict replay/rollups. | Zero mutation calls, replay mismatches, capture loss, state errors, or authority overlap. |
| Jul 26 | Rehearsal 1 against a copied production DB and fake CLN RPC. | Complete cutover and rollback sequence without touching live policy. |
| Jul 27-29 | Fix discovered bugs, redeploy, and restart soak for each candidate. | Required 24- or 72-hour post-fix soak is green. |
| Jul 30 | Rehearsal 2 with restart recovery, stale arm, wrong SHA, Python-still-authoritative, RPC failure, ambiguous outcome, and rollback injections. | Every unsafe case fails closed and the valid fake-RPC case succeeds exactly once. |
| Jul 31-Aug 2 | Freeze the release candidate; no feature work. | Final 72 continuous hours are green. |
| Aug 3 | Manual go/no-go and fee-subsystem cutover. | All gates green, exact artifacts staged, rollback verified, and explicit operator approval given. |

If implementation or a required soak finishes late, the cutover date moves.
The controller reports `NO-GO` rather than compressing or waiving a gate.

## Rehearsal design

Rehearsals use:

- an immutable copy of the production database;
- a copied Rust observer database when restart behavior is under test;
- a fake CLN Unix-socket endpoint that records requests and injects outcomes;
- the exact release candidate binaries and schemas;
- a synthetic, short-lived arm bound to the fake node and test binary.

The harness covers normal authorization, Python-still-authoritative denial,
expired/not-yet-valid arms, wrong node/commit/hash, state-flush failure,
governor denial, ledger failure, RPC rejection, timeout before submission,
ambiguous outcome after submission, restart into quarantine, reconciliation,
and rollback. No rehearsal points the action adapter at the live Lightning
socket.

## Deployment sequence

### Python handoff release

1. Implement and test the default-true authority switch in an isolated Python
   worktree.
2. Run focused fee-path and authority tests, then the full Python suite.
3. Commit and publish the reviewed source.
4. Stage a checksummed rollback copy on `lnnode`.
5. Deploy with fee authority still true.
6. Restart only the Python dynamic plugin if required by its established
   deployment procedure.
7. Verify fee-loop health, authority readback true, Rust still shadow, and no
   behavioral drift.

### Rust shadow release and timers

1. Implement and test in an isolated Rust worktree.
2. Run formatting, clippy, debug and release workspace tests, harness
   self-tests, fake-RPC rehearsal, and source-surface safety checks.
3. Commit and publish the reviewed source.
4. Build the release binaries locally and record source commit, SHA-256, size,
   file type, and dynamic dependencies.
5. Stage and checksum the Rust plugin, replay tool, runway controller, unit
   files, and rollback artifacts on `lnnode`.
6. Enable and verify lingering for `lightningd`; do not continue if the user
   manager cannot survive logout.
7. Stop only the Rust dynamic plugin, atomically replace it, and restart it
   explicitly in observer/dry-run/no-broadcast mode.
8. Verify Python remains authoritative and healthy, Rust's production DB
   descriptors are read-only, the mutation count is zero, and all artifact
   hashes match.
9. Install and enable the two `lightningd` user timers.
10. Run both services once manually, inspect their reports, log out and verify
   the user manager and timers remain active, and then let the timers own
   polling and daily rollup.

Neither deployment creates a cutover arm or disables Python authority.

## Manual cutover and rollback boundary

The timer's final green report is necessary but not sufficient. The operator
must separately approve cutover, verify current live health, create a fresh arm
for the exact installed binary, and execute the documented subsystem handoff.

The cutover order is:

1. freeze configuration changes and take a final green snapshot;
2. disable Python fee authority persistently with `setconfig
   transient=false`, verify positive readback, and verify the returned
   `config.setconfig` source;
3. verify Rust remains dry-run and produce one final would-broadcast batch;
4. install the fresh release-bound arm;
5. switch Rust from shadow to live fee mode, which validates and consumes the
   arm;
6. observe the first bounded live batch and reconcile its outcomes;
7. verify the arm no longer exists and that a Rust restart without a fresh arm
   fails closed rather than reacquiring authority.

Rollback is ordered to prevent authority overlap:

1. disable Rust fee broadcasts and verify no batch is active;
2. remove the arm;
3. reconcile or quarantine every ambiguous Rust action;
4. restore the checksummed prior Rust shadow artifact if needed;
5. re-enable Python fee authority and verify positive readback;
6. confirm Rust is observer/dry-run and mutation count is stable;
7. preserve the incident window and reset promotion status.

The exact re-arm commands are pinned in the implementation runbook after the
final interfaces exist. The timer never performs these steps.

## Testing strategy

Implementation follows test-driven development with these layers:

- **Pure/unit:** request serialization, validation, arm parsing, binary hash
  binding, mode matrix, authority response validation, trigger coalescing,
  state generations, retention selection, gate classification, and schedule
  transitions.
- **Database:** transactional migrations, SeedOnce recovery, atomic flush,
  mempool moving average, quarantine restoration, ledger reconciliation, and
  report identity against temporary SQLite files.
- **Fake RPC integration:** exact `setchannel` payloads, zero calls from the
  shadow executor, one call from a valid live authorization, denial for every
  invalid gate, and ambiguous-outcome quarantine.
- **Python cross-repo:** every scheduled, triggered, and manual fee mutation
  path is blocked when authority is false; read-only/reporting paths remain
  available; default true preserves existing behavior.
- **Replay/parity:** strict captured-envelope replay, config/read-RPC parity,
  and autonomous-state divergence reporting.
- **Timer/controller:** idempotence, overlapping-run lock, timeout, missed-run
  accounting, atomic JSON/Markdown publication, candidate reset, hard
  retention cap, green/yellow/red classification, and no-cutover capability.
- **Rehearsal:** copied production state and fake CLN endpoint with failure and
  rollback injection.
- **Live shadow:** exact artifact provenance, Python sole authority, Rust
  observer/dry-run, read-only production DB descriptors, zero Rust mutation
  count, healthy daily captures, and sustained clean-soak evidence.

The old no-`setchannel` symbol test is replaced by a stricter action-surface
allowlist: only the guarded adapter may contain or invoke `setchannel`, tests
prove the shadow construction graph cannot reach it, and the live shadow
audit proves its invocation count remains zero.

## Completion criteria

Cutover preparation is complete only when:

- all approved Rust and Python work is committed and published;
- both deployment artifacts exactly match their tested sources;
- Python exposes a verified default-on authority switch and Rust positively
  verifies its disabled state before live batches;
- all cutover components run in live shadow, including SeedOnce persistence,
  Rust mempool recording, triggers, governor, ledger, and exact request
  construction;
- both `lightningd` user timers are enabled and producing valid rolled-up
  reports, and the user manager has been verified persistent after logout;
- two copied-state/fake-RPC rehearsals pass, including rollback and every
  fail-closed injection;
- the frozen candidate has 72 continuous green hours;
- rollback artifacts and commands are verified;
- the final report says `GO` and the operator separately approves the manual
  cutover.

Until every criterion is met, Python remains the fee authority and Rust stays
in shadow.
