# Runbook — Rust fee cutover

Operating contract for the Rust fee subsystem: the three accepted modes, the
manual handoff that moves fee authority from Python to Rust, and the rollback.

**Scope of the current deployment: autonomous shadow only.** Nothing in the
"live authority" sections below is authorized by the plan that produced this
document. They are written now, while the code is fresh, so the cutover is
performed from a checked document rather than from memory.

Two things this runbook cannot do, by construction:

- **A timer can never create an arm.** Arm creation is a manual `install(1)`
  by an operator. No unit, timer, or scheduled job in this repository writes
  one.
- **A timer can never change authority.** Authority is decided once, at plugin
  init, from options that are not `.dynamic()`. There is no runtime path that
  promotes a running shadow to live.

---

## 1. The mode matrix

Four booleans decide the mode. They are read **once**, at init, and validated
by `validate_fee_mode` (`crates/revops/src/fee_mode.rs`). Exactly three
combinations are accepted; every other combination refuses startup.

| `observer` | `fee-dryrun` | `fee-broadcast` | `fee-stateful-shadow` | arm | Mode |
|---|---|---|---|---|---|
| `true` | `false` | `false` | `false` | must be absent | **Passive observer** |
| `true` | `true` | `false` | `true` | must be absent | **Autonomous shadow** |
| `false` | `false` | `true` | `false` | **required** | **Live authority** |

Option names are `revops-r-<suffix>` by default, or `revenue-ops-<suffix>` when
`REVOPS_CANONICAL_NAMES=1` (i.e. Python unloaded). The suffixes above are
`observer`, `fee-dryrun`, `fee-broadcast`, `fee-stateful-shadow`, plus
`cutover-arm-path`, `db-path`, `observer-db-path`, `journal-dir`,
`flow-window-days`.

### Additional row-level preconditions

- **Autonomous shadow** requires seed provenance: either the store is virgin
  (`PendingFirstCycle` — it will seed on its first cycle) or a seed event row
  exists (`AlreadySeeded`). Committed state with **no** seed event refuses
  startup (`NeverSeeded`) — that combination means the provenance record was
  lost, and a shadow that cannot say where its state came from is not evidence.
- **Live authority** requires `generation > 0` **and** a seed event. A virgin
  or unseeded store refuses (`LiveModeRequiresSeededState`): live must never be
  the first thing to touch the Rust-owned store.
- An arm supplied in a **non-live** mode refuses (`ArmPresentInNonLiveMode`).
  Setting `cutover-arm-path` does not "prepare" a shadow; it breaks it.

### Two consequences operators hit first

- **`fee-dryrun` is not a live off-switch.** None of the five mode options is
  `.dynamic()`. `setconfig revops-r-fee-dryrun false` is accepted by lightningd
  and has **no effect** on a running plugin. The fee-cycle kill switch is
  `plugin stop`, which works only because the plugin manifest as a whole
  advertises `dynamic: true` (`main.rs:711`). Do not remove that.
- **A filesystem that cannot do WAL refuses startup.** The observer database is
  opened with `busy_timeout` and a *verified* `journal_mode=WAL`; a silent
  fallback to a rollback journal is a hard error naming the file. On NFS or
  some overlay filesystems this takes the fee scheduler dark with a loud log
  rather than degrading invisibly. That direction is deliberate: in rollback
  mode a concurrent reader blocks the fee-cycle writer.

---

## 2. Autonomous shadow (the currently authorized deployment)

### 2.1 Options

Exactly these, and nothing else:

```text
revops-r-observer=true
revops-r-fee-dryrun=true
revops-r-fee-stateful-shadow=true
revops-r-fee-broadcast=false
revops-r-cutover-arm-path=      # empty
```

`revops-r-fee-stateful-shadow=true` **must** be present in the plugin's start
arguments. A node already running `observer=true` + `fee-dryrun=true` without
it matches no accepted row, so the plugin will refuse to start — this is the
single most likely deployment failure.

### 2.2 Deploy

Use the checksummed atomic replacement in
`docs/audit/2026-07-19-shadow-parity-deployment-closeout.md`: stage the binary,
verify its hash, keep a rollback copy, then `plugin stop` / `mv` /
`plugin start` with keyword arguments. Restart **only** the Rust dynamic
plugin. Never touch Python's fee authority.

A new binary is a new candidate, and a new candidate starts a fresh 72-hour
soak measured daily by `tools/diff-harness/engagement_gate.py`.

### 2.3 Post-deploy verification

- Installed and running checksum match the artifact you built.
- `revops-fee-runway-status` returns, and reports seed provenance.
- Python fee authority still reports `enabled=true`.
- Rust state generation advances cycle over cycle.
- Restart the Rust plugin once: it must **resume** from the Rust generation,
  not reseed from Python.
- Fresh mempool evidence, trigger receipts, governor and ledger audit rows.
- Production database descriptors are **read-only**.
- **Zero** quarantine rows and **zero** mutation calls.

Expect the first cycle after deploy to be non-comparable (bootstrap). The
engagement gate must go green from cycle 2.

### 2.4 Engagement-gate cautions

- Point `--observer-db` at the **live** file on the node, never a copy. The
  gate has no freshness assertion, and a WAL-less copy silently truncates the
  tail without producing a cadence gap.
- From candidate #2 onward, join `rust_fee_cycles` and assert exactly one
  `binary_sha256` in the window. A too-early `--since` otherwise blends two
  candidates into one verdict.

---

## 3. Manual handoff to live authority (NOT authorized by this plan)

### 3.1 The arm

A cutover arm is a one-time, single-session capability. Schema
`revops_fee_cutover_arm/v1`, subsystem `fees`, file mode `0600` (anything
group- or world-accessible is refused — a looser mode is evidence the
deployment procedure was not followed).

It binds, and is checked against, the running process:

- `node_id` — the node it authorizes
- `source_commit` — the reviewed commit
- `binary_sha256` — the exact binary
- validity window — `not_before` / `not_after`
- `nonce` — the replay identity

Wrong node, wrong commit, wrong hash, too early, or expired: **denied, and the
arm is not consumed** — every content check fails before the rename.

### 3.2 Consumption is a rename, and it is one-way

A validated arm is consumed by `RENAME_NOREPLACE` into
`<journal-dir>/cutover-consumed/<nonce>`. That directory **is** the replay
ledger. It is pinned to `journal-dir`; if `journal-dir` cannot be resolved,
startup hard-refuses rather than falling back to a relative path — a split
ledger means one nonce is consumable twice. The consumed directory must be on
the **same mount** as the arm, or the rename is not atomic.

### 3.3 Order of operations — the trap

`resolve_startup_mode` consumes the arm at step 2, **before**
`validate_fee_mode` denies at step 3.

Therefore: **verify the store is seeded BEFORE the arm file exists on disk.**
Not "before setting `fee-broadcast=true`". Placing an arm for a node that has
not seeded destroys that one-time arm on a startup that then refuses anyway,
and you must mint a fresh one.

The same shape applies to transient failures: the arm is consumed before
`ClnFeeBroadcaster::new` runs, so a transient observer-DB error at startup
**burns** the arm. This is fail-closed and the correct direction, but the
operator must expect to mint again.

### 3.4 What live mode can and cannot do

- The **only** mutating RPC in the entire binary is `setchannel`, at exactly
  one call site (`fee_execution.rs:808`), reachable only through the guarded
  broadcaster, which is constructible only under `LiveAuthority`
  (`main.rs:1562`). `tests/action_surface.rs` enforces this structurally.
- Every batch requires a fresh `LiveBatchAuthorization`, consumed by value —
  it cannot be replayed.
- Authorization requires **two genuinely separate** `PythonAuthorityClient`
  fetches showing Python authority off. One reading is a witness, not a
  verifier.
- Python must report authority **disabled** before Rust may broadcast. Both
  enabled is a misconfiguration, not a race to win.

### 3.5 Quarantine

`Ambiguous` — bytes may have reached lightningd but no definite answer came
back — is the **only** outcome that quarantines. `Rejected` and `CleanFailure`
are terminal and never quarantine.

- A failure to *persist* a quarantine poisons the broadcaster in-process: it
  refuses every further batch, immediately, with zero calls, until restart.
- On restart, `ClnFeeBroadcaster::new` reconciles quarantine **before**
  accepting any arm, and refuses construction if reconciliation fails.
- Reconciliation acts on orphaned broadcast *attempts* — an intent with no
  recorded result, left by a process that exited between submitting and
  recording — and **inserts** a quarantine. It does not clear one. Clearing is
  a deliberate operator action after establishing what actually reached the
  node.

### 3.6 Rollback

Reverse of deploy, and the rehearsal harness asserts the order from the
filesystem rather than from a log line: stop the Rust plugin, restore the
previous binary from the rollback copy, verify its checksum, restart with the
**shadow** option row from §2.1, re-enable Python authority, and confirm
`revops-fee-runway-status` plus Python's authority readback agree. Residue must
be empty.

---

## 4. Rehearsing before doing any of this

`rehearse_fee_cutover` runs the real code paths against copied databases and a
fake socket it binds itself. It has no default root, refuses any path bearing a
production marker before opening anything, and mints arms bound to a synthetic
node id with zeroed commit and hash — so an arm it creates can never validate
in production.

```bash
cargo run -p revops --bin rehearse_fee_cutover -- --list-scenarios
cargo run -p revops --bin rehearse_fee_cutover -- \
  --rehearsal-root /tmp/rv-check --scenario valid_activation
```

Keep the root short: the derived socket path must fit `sockaddr_un`
(under 108 bytes), and an oversized root is refused for every scenario.

Each scenario asserts the **exact** error variant it claims to exercise and
refuses anything else, so an outcome can never be headlined by a run that did
not reach it. Gate denials are proven by zero requests received at the fake
socket; transport outcomes by exactly one. `--inject-fault` exists to drive
that refusal deliberately; a run that used it records `injected_fault` in its
evidence and can never be mistaken for a clean rehearsal.
