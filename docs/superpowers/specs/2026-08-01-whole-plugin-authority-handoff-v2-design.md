# Whole-Plugin Python→Rust Authority Handoff — v2 Arm Design

**Date:** 2026-08-01
**Task:** hexmem 69 (design-only checkpoint; supersedes the fee-only
`revops_fee_cutover_arm/v1`)
**Status:** DESIGN ONLY. This document specifies behavior. It authorizes no
code, no arm issuance, no deployment, no Python shutdown, no live contact,
and no authority change.

---

## 0. Why v1 cannot be extended

`crates/revops/src/cutover_arm.rs` (v1) is a sound *file* gate — mode 0600,
owner match, symlink refusal, node/subsystem/commit/binary binding,
not-before/expiry, single-use nonce with a deny-ledger. It is nonetheless
unusable as the whole-plugin arm for three structural reasons:

1. **Fee-only subsystem binding.** Its `subsystem` field names one
   subsystem. A whole-plugin handoff must bind the *entire* canonical
   surface at once; a v1-shaped arm that names one subsystem is, by
   construction, a *subset* arm (§7.1 denies these).
2. **Consumed too early.** `validate_and_consume` burns the nonce before
   the capability set is constructed. A construction failure after the burn
   leaves the operator with a spent arm and no authority — recoverable only
   by issuing a new arm, which is precisely the state a failed transition
   must *not* require. v2 inverts this: consume is the last reversible step
   before an all-or-none construction that cannot fail for reasons the
   preflight could have detected.
3. **The live runtime is inert.** v1 grants a capability nothing consumes.
   A v2 arm that "succeeds" while every owner remains unassembled would be
   a false success — the exact failure class this port refuses everywhere
   else.

v2 is therefore a new mechanism. v1 stays in the tree only until v2 lands,
then is deleted; it must never be reachable from the v2 path.

---

## 1. The namespace/handoff contradiction, resolved

**The contradiction.** Python owns the 69 canonical `revenue-*` RPC names.
lightningd permits exactly one registrant per method name. Rust in canonical
mode wants those same names. So Rust cannot hold the canonical namespace
while Python is running — yet the proof that Python's authority is off must
be verifiable *by Rust*, *after* the namespace moves, and must never be
inferred from a name's absence (§7.2).

**Resolution: a durable positive handoff receipt plus a non-colliding
sentinel.**

### 1.1 The handoff receipt (positive proof, epoch-bound)

Python writes a `authority_handoff` row into the **production database**
inside one `BEGIN IMMEDIATE` transaction as the final act of its
authority-off sequence, *before* it unregisters anything:

| column | meaning |
|---|---|
| `epoch` | monotonic integer; strictly greater than any prior row |
| `node_id` | the node this receipt is about |
| `released_at` | unix seconds, written from Python's clock |
| `released_by` | Python release identity: source commit + file digest |
| `authority_state` | the literal `python_authority_off` |
| `nonce_binding` | the v2 arm nonce this release is FOR |
| `observer_db_id` | `(device, inode)` of the Rust observer DB Python was told to expect |
| `nonce_ledger_id` | `(device, inode)` of the nonce deny-ledger |
| `receipt_digest` | sha256 over every field above, canonically serialized |

The receipt is **positive** (a written assertion, not an absence),
**epoch-bound** (a stale receipt from an earlier cutover attempt cannot be
replayed — §7.5), and **nonce-bound** (a receipt written for arm *A* cannot
authorize arm *B*).

Python must also set its own runtime authority gate to disabled *before*
writing the receipt, so the receipt is never more permissive than reality.

### 1.2 The sentinel RPC

Rust registers exactly one non-colliding method, `revops-handoff-status`,
at every startup in every mode. It is read-only, never renamed, and never
part of the canonical set, so it can be queried while Python still owns
`revenue-*`. It reports: current mode, release identity, preflight verdict
with per-gate detail, capability state (`unassembled` | `live`), and the
epoch of the receipt it last observed. This is how an operator and any
verifier observe the transition without depending on a canonical name
appearing or disappearing.

### 1.3 Ordering

```
1. Operator approves.                      (out of band)
2. Python sets authority gate = off.       (Python)
3. Python writes the handoff receipt.      (Python, BEGIN IMMEDIATE)
4. Python unregisters / stops.             (Python)
5. Rust preflight (§3) — reads the receipt, verifies everything.
6. Rust re-verifies (§4.2) and consumes the arm (§4.3).
7. Rust constructs the capability set all-or-none (§5).
8. Rust registers the canonical namespace.
9. Rust postflight (§6).
```

Steps 2–4 are Python's; 5–9 are Rust's. **There is no interval in which
both hold mutation authority**: Python's gate is off before its receipt
exists, and Rust cannot construct capability before it reads that receipt.
There *is* an interval in which *neither* holds authority (between 4 and 7).
That interval is deliberate, is bounded by the postflight deadline, and is
the safe direction to fail.

---

## 2. The embedded clean-release manifest

The binary embeds, at build time, a `ReleaseManifest`:

- `source_commit` — exact git commit, refused if the tree was dirty
- `binary_sha256` — self-digest, recomputed at runtime and compared
- `inventory_digest` — sha256 of the canonical `plugin_inventory.json`
- `rpc_contract_digest` — sha256 of `fixtures/port/rpc_params.json`
- `canonical_rpc_names` — the exact sorted 69-name set
- `subsystem_set` — the exact sealed set of owners the capability constructs

`inventory_digest` and `rpc_contract_digest` bind the *generated* contract,
so a binary built against a different Python surface cannot arm. The arm
file carries the manifest digest it expects; a mismatch denies (§7.3).

---

## 3. Preflight (strict, snapshot, no mutation)

Preflight takes **one snapshot** and evaluates every gate against that
snapshot. It performs no writes of any kind — not to the production DB, not
to the observer store, not to the nonce ledger, not to the arm file.

| # | Gate | Denies when |
|---|---|---|
| P1 | Release identity | running binary digest ≠ embedded `binary_sha256`, or commit mismatch |
| P2 | Inventory binding | `inventory_digest` or `rpc_contract_digest` ≠ embedded |
| P3 | Canonical set | the registered-name plan ≠ the exact 69-name set (superset or subset both deny, §7.1) |
| P4 | Handoff receipt | absent, digest invalid, `authority_state` ≠ `python_authority_off`, epoch ≤ last-consumed epoch, or `nonce_binding` ≠ this arm's nonce |
| P5 | Store identity | production DB, observer DB, and nonce ledger `(device, inode)` ≠ the receipt's and the arm's bindings |
| P6 | Production DB posture | production handle is not read-only at preflight time |
| P7 | Observer store health | schema/integrity check fails, or the store is not this boot's |
| P8 | Current-boot loops | any required loop has no completed pass **this boot** (§8) |
| P9 | Reconciliation | any owner reports unreconciled orphan intents, or ambiguous (unknown-outcome) records exist |
| P10 | Arm file | any v1-class file failure (mode, owner, symlink, schema, node, expiry, not-yet-valid) |
| P11 | Nonce | empty, malformed, already in the deny-ledger, or already consumed |
| P12 | Evidence completeness | ANY required gate input is null/absent rather than measured (§7.4) |
| P13 | Placeholder scan | any canonical response builder still emits a placeholder/`not_yet_ported`/Rust-only success vocabulary (§7.6) |

Preflight returns a **typed verdict per gate**, never a bare bool. The
sentinel RPC surfaces the full verdict set so a red gate is attributable
without re-running anything.

**Consumed-before-preflight denies (§7.7):** if the arm's nonce already
appears in the deny-ledger when preflight *starts*, preflight denies. There
is no path where consumption precedes the full gate set.

---

## 4. Parse → re-verify → burn → consume

### 4.1 Parse without mutation
The arm file is parsed into an in-memory `ParsedArm`. Parsing writes
nothing. A malformed arm denies here.

### 4.2 Re-verify immediately before consumption
Every gate in §3 that can change between preflight and consumption is
**re-evaluated** against a fresh read: the handoff receipt (P4), store
identities (P5), production DB posture (P6), current-boot loop state (P8),
reconciliation state (P9), and nonce state (P11). Re-verification failure
denies with the nonce **unburned**.

The re-verify window is the only TOCTOU surface, and it is deliberately
narrowed to: read receipt → read ledger → burn → consume, with no I/O
between them other than those operations.

### 4.3 Durable nonce burn, then atomic arm consume
1. **Burn:** insert the nonce into the deny-ledger and `fsync`. The burn is
   durable *before* the arm file is touched. A crash here leaves a burned
   nonce and an unconsumed arm — the safe direction: the arm is now inert
   and a fresh nonce is required.
2. **Consume:** atomically `rename` the arm file to its consumed path. A
   crash here leaves a burned nonce and a consumed arm — also safe.

Never the reverse order: consuming first would permit a replay window in
which a copied arm file is re-presented against an unburned nonce.

---

## 5. `WholePluginLiveCapability` — one, non-cloneable, all-or-none

```
pub struct WholePluginLiveCapability {
    _seal: (),          // private: unconstructible outside its module
}                       // NOT Clone, NOT Copy, NOT Default, NOT Serialize
```

- **One constructor**, taking the `ConsumedArm` by value (so it cannot be
  called twice from one arm) and returning `Result<SealedOwnerSet, Denial>`.
- **Exact set.** The constructor builds *every* owner named in
  `subsystem_set` — fee broadcaster, core-state mutation owner, rebalance
  engine, capital/planner adapters, Boltz action capability, LN+ writer,
  datastore producers. A subsystem missing from the set, or present but not
  named, denies (§7.1).
- **All-or-none.** Construction is staged: every owner is *built* into a
  local, then the whole set is moved into place in one step. Any single
  failure drops every local and returns `Denial` — no partially-live plugin
  can exist, not even transiently.
- **Store-verified typed capabilities.** The governor and reservation
  capabilities are not booleans; each is a typed handle whose constructor
  re-reads its backing store and verifies identity and schema. A governor
  capability that cannot prove its ledger identity refuses construction.
- **Exact action batch digests.** Every action batch the capability can
  execute carries a digest of its *intended* action set, computed at
  construction. An owner presented with a batch whose digest does not match
  refuses it. This makes "the capability executed something it was not
  constructed for" unrepresentable rather than merely unlikely.

The capability is *the* proof of authority: no owner performs a mutation
without holding a reference to it, and it exists only after §4 completes.

---

## 6. Postflight (strict)

After construction and canonical registration:

| # | Check | Fails when |
|---|---|---|
| Q1 | Namespace ownership | the 69 canonical names are not all registered *by this process* |
| Q2 | No stragglers | any Rust-only name collides with the canonical namespace |
| Q3 | Python absence | Python's authority gate still reads enabled, or a newer receipt appeared |
| Q4 | Store posture | production DB is now writable by the intended owner and no other holder is detected |
| Q5 | Capability liveness | every owner in the sealed set reports live and reconciled |
| Q6 | First-cycle sanity | the first governed cycle completes without quarantine or persistence failure |

Postflight failure triggers rollback (§7). Postflight is not advisory: an
unverified postflight is a failed postflight.

---

## 7. Denial matrix (every one of these MUST deny)

| id | Condition | Where caught |
|---|---|---|
| D1 | **Superset arm** — arm names a subsystem outside `subsystem_set` | P3/§5 |
| D2 | **Subset arm** — arm names fewer than the full set (incl. any v1 fee-only arm) | P3/§5 |
| D3 | **Missing RPC as proof** — authority-off inferred from a canonical name being unregistered | P4 (only the positive receipt counts) |
| D4 | **Matching placeholders** — a canonical response that merely *looks* right (placeholder, `not_yet_ported`, fabricated zero, Rust-only success vocabulary) | P13 |
| D5 | **Nullable evidence** — any required gate input null/absent instead of measured | P12 |
| D6 | **Consumed before preflight** — nonce already burned when preflight starts | P11/§3 |
| D7 | **Replayed receipt** — epoch ≤ last consumed epoch | P4 |
| D8 | **Cross-bound receipt** — receipt's `nonce_binding` ≠ this arm's nonce | P4 |
| D9 | **Store swap** — any store `(device, inode)` differs from binding | P5/§4.2 |
| D10 | **Stale loop evidence** — a pass from a *prior* boot presented as current | P8 |
| D11 | **Ambiguity present** — unknown-outcome records exist at arm time | P9 |
| D12 | **Dirty release** — binary/commit/inventory/contract digest mismatch | P1/P2 |
| D13 | **Double construction** — a second capability construction attempt | §5 (arm consumed by value) |
| D14 | **Post-rollback reuse** — the same nonce presented after a rollback | §8.4 |

---

## 8. Rollback (ambiguity-aware, ordered)

Rollback order is **not** the reverse of acquisition. It is:

1. **Destroy Rust mutation capability FIRST.** Drop the sealed owner set and
   the `WholePluginLiveCapability`; every owner's action seam becomes
   unassembled. Nothing else happens until this completes. Rust must lose
   the ability to mutate before anything else is touched — a rollback that
   restored Python first would create the split-authority interval the
   whole design exists to prevent.
2. **Preserve ambiguity and reservations.** Unknown-outcome records, in-flight
   reservations, and quarantine entries are *preserved verbatim*, never
   cleaned up, released, or resolved by rollback. Rollback must not decide
   the outcome of an operation it does not know the outcome of.
3. **Verify Rust authority absence.** Positively check: no capability
   exists, every owner reports unassembled, the production handle is
   read-only again, and no Rust write has occurred since the rollback
   started. Absence is *verified*, not assumed.
4. **Then restore Python authority.** Only after step 3 passes. Python's
   restart re-enables its gate and writes its own resumption receipt with a
   fresh epoch.
5. **Require a fresh nonce.** The rolled-back nonce is permanently in the
   deny-ledger. Any subsequent attempt requires a newly issued arm *and*
   explicit operator approval (D14). A coordinated rollback never leaves a
   reusable arm.

If step 3 cannot be verified, rollback **stops and escalates** rather than
restoring Python — a state where both sides might write is worse than a
state where neither does.

---

## 9. RED / mutation matrix

Every row is a test that must exist and must fail RED before its
implementation, and whose mutation must be killed by a TEST failure.

### 9.1 Arm & nonce
| Test | Mutation that must die |
|---|---|
| superset arm denies | accept unknown subsystem |
| subset/fee-only arm denies | accept partial `subsystem_set` |
| consumed-before-preflight denies | check ledger after preflight instead of before |
| burn precedes consume | swap burn/consume order |
| crash between burn and consume leaves arm inert | make burn non-durable (skip fsync) |
| re-verify failure leaves nonce unburned | burn before re-verify |
| replayed nonce denies | drop the deny-ledger lookup |

### 9.2 Receipt & namespace
| Test | Mutation that must die |
|---|---|
| absent receipt denies | treat missing receipt as authority-off |
| missing canonical RPC is NOT proof | infer authority-off from name absence |
| stale epoch denies | drop the epoch comparison |
| cross-bound nonce denies | drop `nonce_binding` check |
| receipt digest tamper denies | skip digest recomputation |
| sentinel RPC never collides | rename sentinel into `revenue-*` |

### 9.3 Capability
| Test | Mutation that must die |
|---|---|
| all-or-none: one owner fails ⇒ none live | move owners into place incrementally |
| double construction denies | take `ConsumedArm` by reference |
| governor capability verifies its store | construct without re-reading identity |
| action batch digest mismatch refuses | skip digest comparison |
| capability is not cloneable | derive `Clone` |

### 9.4 Preflight evidence
| Test | Mutation that must die |
|---|---|
| null required evidence denies | `unwrap_or_default()` on a gate input |
| prior-boot loop pass denies | compare generation without boot id |
| ambiguity present denies | ignore unknown-outcome records |
| placeholder scan denies | allow `not_yet_ported` in canonical responses |
| dirty release denies | skip binary self-digest |

### 9.5 Rollback
| Test | Mutation that must die |
|---|---|
| capability destroyed before Python restore | reorder to restore Python first |
| ambiguous reservations preserved | release/clean them during rollback |
| unverifiable absence escalates | proceed to Python restore anyway |
| post-rollback nonce reuse denies | clear the deny-ledger on rollback |

---

## 10. Explicit non-goals

- No automatic cutover. Every transition requires explicit operator approval
  immediately before consumption.
- No partial/subsystem-at-a-time authority. The design has exactly two
  states: Python-authoritative, or Rust-authoritative.
- No self-healing. A failed transition stops and escalates; it never retries
  itself.
- The deadline is not evidence. A red, unknown, or stale gate stops the
  cutover regardless of date.

---

## 11. Open items for the reviewer

1. **Receipt location.** This design puts the handoff receipt in the
   production DB (one store both sides already bind and can transact
   against). A dedicated file would avoid a Python write to a store Rust is
   about to own; a file, however, has weaker atomicity than
   `BEGIN IMMEDIATE`. Decision recorded as DB-with-immediate-txn; flag if
   the reviewer disagrees.
2. **Neither-authority interval.** Bounded by the postflight deadline but
   nonzero. Alternative designs that shrink it all reintroduce a
   both-authority risk. Recorded as an accepted trade.
3. **Python-side work.** Steps 2–4 of §1.3 are Python changes (gate-off
   ordering, receipt writer, resumption receipt). They are NOT in this
   repository and must be specified and reviewed on the Python side before
   any rehearsal.
