//! Transactional Rust-owned fee state + audit schema (stateful-shadow
//! plan Task 4). Lives entirely in the observer db (`owner.rs` -- writable,
//! Rust-owned, never the Python production database, see this module's and
//! `owner.rs`'s doc comments). Nothing here is wired to live RPC authority;
//! this module only persists state/audit rows the fee scheduler produces.
//!
//! # Table inventory (runway design spec "Rust-owned persistence" bullets)
//!
//! - `rust_fee_state_generation` / `rust_fee_state` -- controller state and
//!   state generations. One row per channel at the CURRENT generation only
//!   (an `INSERT ... ON CONFLICT DO UPDATE` overwrite, not a full history);
//!   `v2_state_json` carries the same lossless envelope shape the Phase 4
//!   Task 9 production gate proved byte-exact
//!   (`revops_fees::state_store`/`fee_state.rs::state_envelope`) -- this
//!   crate does not depend on `revops-fees`, so the envelope is passed in
//!   already serialized by the caller.
//! - `rust_fee_cycles` -- shadow cycle summaries (one row per committed
//!   cycle; `generation` pins the state generation that cycle produced).
//! - `rust_fee_requests` -- attempted request/audit rows: one row per
//!   would-broadcast prepared fee action (`PreparedFeeAction` in
//!   `revops-fees`), `UNIQUE(cycle_id, channel_id)` since a cycle produces
//!   at most one decision per channel.
//! - `rust_fee_shadow_outcomes` -- shadow cycle TERMINAL OUTCOMES: one row
//!   per (cycle, channel) decision, broadcasts AND skips alike. This is
//!   the "shadow-cycle-outcome" table the 2026-07-26 revision plan's Task
//!   R7/R8 amendment binds a column contract to (see
//!   [`ShadowCycleOutcomeRow`]'s doc comment) -- NOT explicitly named in
//!   the original Task 4 brief's table list, but required by the runway
//!   design spec's "shadow cycle summaries and terminal outcomes" bullet
//!   and the amendment; added as a 10th table alongside the brief's 9.
//! - `rust_mempool_fee_history` -- Rust's own decision-relevant mempool
//!   sample cadence (cutover reads only these, never Python's).
//! - `rust_mempool_ma_comparison` -- fix round 1 (review finding 1): the
//!   shadow-window comparison of Rust's 24h mempool moving average
//!   against Python's (R8 binding constraint), persisted instead of only
//!   logged -- the daily rollup consumes DB evidence, never logs. One row
//!   per `RehydratePerCycle` comparison; `python_ma`/`delta` are `NULL`
//!   when Python's MA could not be read that cycle (recorded anyway, so
//!   absence is itself evidence, never a skipped row) -- NOT in the
//!   original Task 4/6 brief's table list, added here for the same
//!   "extend, don't duplicate" reason `rust_fee_shadow_outcomes` was.
//! - `rust_fee_trigger_events` -- trigger receipts and coalescing
//!   decisions (`forward_event`, failed-forward nudges, policy changes,
//!   wake-all, Vegas spike checks, fixed-interval scheduler).
//! - `rust_fee_ledger` -- governor AND dry-run ledger outcomes, unified
//!   into one physical table via the `kind` discriminator (`'governor'`
//!   uses `authorized`/`reason_code`; `'ledger'` uses
//!   `event_type`/`snapshot_id`/`details_json`) -- matches the design
//!   spec's single "governor and dry-run ledger outcomes" bullet while
//!   still carrying `FeeCycleCommit`'s two distinct `Vec` fields
//!   (`governor: Vec<GovernorAuditRow>`, `ledger: Vec<LedgerAuditRow>`).
//! - `rust_execution_quarantine` -- execution quarantine + reconciliation
//!   state. An ambiguous post-submission transport outcome quarantines all
//!   subsequent broadcasts and is never retried automatically (Global
//!   Constraints) -- this module only offers insert/query; no automatic
//!   clear.
//! - `rust_runway_snapshots` -- timer snapshots and report identity.
//!
//! # Transactional contract
//!
//! [`commit_fee_cycle`] is the ONLY writer of `rust_fee_state`,
//! `rust_fee_state_generation`, `rust_fee_cycles`, `rust_fee_requests`,
//! and `rust_fee_ledger`; all five are written inside ONE `BEGIN
//! IMMEDIATE` transaction (plus `rust_fee_shadow_outcomes`) so a failure
//! anywhere (a duplicate request identity, a constraint violation, a
//! disk error) rolls every one of them back together -- the previous
//! generation and every previously-committed row are left byte-for-byte
//! unchanged. Foreign keys (`PRAGMA foreign_keys = ON`, set in
//! [`init_schema`]) additionally reject any request/ledger/outcome row
//! that names a `cycle_id` with no parent `rust_fee_cycles` row.
//!
//! Schema migration ([`init_schema`]) is idempotent (`CREATE TABLE IF NOT
//! EXISTS`) and never targets the Python production database -- see
//! `owner.rs`'s module doc comment; this module is only ever handed a
//! connection to the plugin's own observer db file.

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};

use crate::retention::{
    RetentionCursor, RetentionReport, RETENTION_BATCH_ROWS, RETENTION_MAX_BATCHES_PER_SWEEP,
    RUNWAY_EVIDENCE_RETENTION_SECONDS, SNAPSHOT_KEEP_LAST,
};

/// Idempotent `CREATE TABLE IF NOT EXISTS` (+ enabling foreign key
/// enforcement, a per-connection SQLite setting) for every Task 4 table.
/// Safe to call on every connection open, including a connection that
/// already has these tables.
pub fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(DDL).context("init fee runway schema")?;
    migrate_seed_binding_columns(conn)?;
    Ok(())
}

/// Task 42 correction F1: durable binding from successful seed provenance
/// to the exact generation-1 cycle. Idempotent additive migration: two
/// nullable columns (legacy rows keep NULL = UNBOUND, which validation
/// rejects for authority) plus a partial unique index enforcing AT MOST
/// ONE successful seed row per store, ever (SeedOnce means once — a
/// second success is a conflict by construction).
fn migrate_seed_binding_columns(conn: &Connection) -> Result<()> {
    let mut columns = conn
        .prepare("PRAGMA table_info(rust_fee_seed_events)")
        .context("inspect rust_fee_seed_events schema")?;
    let names: Vec<String> = columns
        .query_map([], |row| row.get::<_, String>(1))
        .context("read rust_fee_seed_events columns")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("collect rust_fee_seed_events columns")?;
    drop(columns);
    if !names.iter().any(|n| n == "bound_cycle_id") {
        conn.execute_batch(
            "ALTER TABLE rust_fee_seed_events ADD COLUMN bound_cycle_id TEXT;
             ALTER TABLE rust_fee_seed_events ADD COLUMN bound_generation INTEGER;",
        )
        .context("add seed binding columns")?;
    }
    conn.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_rust_fee_seed_success_singleton
             ON rust_fee_seed_events(outcome) WHERE outcome = 'seeded';",
    )
    .context("create successful-seed singleton index")?;
    Ok(())
}

const DDL: &str = "
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS rust_fee_state_generation (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    generation INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rust_fee_state (
    channel_id TEXT PRIMARY KEY,
    generation INTEGER NOT NULL,
    v2_state_json TEXT NOT NULL,
    last_update INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rust_fee_state_generation
    ON rust_fee_state(generation);

CREATE TABLE IF NOT EXISTS rust_fee_cycles (
    cycle_id TEXT PRIMARY KEY,
    generation INTEGER NOT NULL,
    started_at INTEGER NOT NULL,
    completed_at INTEGER NOT NULL,
    source_commit TEXT NOT NULL,
    binary_sha256 TEXT NOT NULL,
    request_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rust_fee_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle_id TEXT NOT NULL REFERENCES rust_fee_cycles(cycle_id) ON DELETE CASCADE,
    channel_id TEXT NOT NULL,
    idempotency_key TEXT,
    old_fee_ppm INTEGER NOT NULL,
    new_fee_ppm INTEGER NOT NULL,
    feebase_msat INTEGER NOT NULL,
    htlcmin_msat INTEGER,
    htlcmax_msat INTEGER,
    message TEXT NOT NULL,
    at INTEGER NOT NULL,
    UNIQUE (cycle_id, channel_id)
);
CREATE INDEX IF NOT EXISTS idx_rust_fee_requests_cycle
    ON rust_fee_requests(cycle_id);

CREATE TABLE IF NOT EXISTS rust_fee_shadow_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle_id TEXT NOT NULL REFERENCES rust_fee_cycles(cycle_id) ON DELETE CASCADE,
    cycle_ts INTEGER NOT NULL,
    channel_id TEXT NOT NULL,
    would_broadcast INTEGER NOT NULL,
    has_algorithm_values INTEGER NOT NULL,
    disposition TEXT,
    skip_gate_comparable INTEGER NOT NULL,
    UNIQUE (cycle_id, channel_id)
);
CREATE INDEX IF NOT EXISTS idx_rust_fee_shadow_outcomes_ts
    ON rust_fee_shadow_outcomes(cycle_ts);

CREATE TABLE IF NOT EXISTS rust_mempool_fee_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sampled_at INTEGER NOT NULL,
    sat_per_vbyte REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rust_mempool_fee_history_ts
    ON rust_mempool_fee_history(sampled_at);

CREATE TABLE IF NOT EXISTS rust_mempool_ma_comparison (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at INTEGER NOT NULL,
    cycle_ts INTEGER NOT NULL,
    rust_ma REAL NOT NULL,
    python_ma REAL,
    delta REAL
);
CREATE INDEX IF NOT EXISTS idx_rust_mempool_ma_comparison_at
    ON rust_mempool_ma_comparison(at);

CREATE TABLE IF NOT EXISTS rust_fee_trigger_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trigger_type TEXT NOT NULL,
    channel_id TEXT,
    cycle_id TEXT REFERENCES rust_fee_cycles(cycle_id) ON DELETE SET NULL,
    cycle_ts INTEGER,
    received_at INTEGER NOT NULL,
    coalesced INTEGER NOT NULL DEFAULT 0,
    detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_rust_fee_trigger_events_received
    ON rust_fee_trigger_events(received_at);

CREATE TABLE IF NOT EXISTS rust_fee_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle_id TEXT NOT NULL REFERENCES rust_fee_cycles(cycle_id) ON DELETE CASCADE,
    channel_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('governor', 'ledger')),
    event_type TEXT NOT NULL,
    authorized INTEGER,
    reason_code TEXT,
    intent_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    snapshot_id TEXT,
    at INTEGER NOT NULL,
    details_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_rust_fee_ledger_cycle
    ON rust_fee_ledger(cycle_id);

CREATE TABLE IF NOT EXISTS rust_execution_quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entered_at INTEGER NOT NULL,
    reason TEXT NOT NULL,
    cycle_id TEXT REFERENCES rust_fee_cycles(cycle_id) ON DELETE SET NULL,
    channel_id TEXT,
    request_id TEXT,
    cleared_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_rust_execution_quarantine_active
    ON rust_execution_quarantine(cleared_at);

CREATE TABLE IF NOT EXISTS rust_runway_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_at INTEGER NOT NULL,
    report_schema_version TEXT NOT NULL,
    source_commit TEXT NOT NULL,
    binary_sha256 TEXT NOT NULL,
    summary_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rust_fee_seed_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    seeded_at INTEGER NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('seeded', 'seed_refused')),
    source_db_path TEXT NOT NULL,
    source_max_last_update INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    payload_sha256 TEXT NOT NULL,
    source_commit TEXT NOT NULL,
    refused_channel TEXT,
    refused_field TEXT,
    detail TEXT
);

CREATE TABLE IF NOT EXISTS rust_fee_restart_markers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at INTEGER NOT NULL,
    process_id INTEGER NOT NULL,
    prior_generation INTEGER NOT NULL,
    hydration_source TEXT NOT NULL,
    source_commit TEXT NOT NULL
);

-- Task 9 (stateful-shadow revision): the guarded live broadcaster's own
-- intent/result ledger. `outcome IS NULL` means an intent was persisted
-- BEFORE any socket write but no result was ever recorded -- either the
-- process is still mid-flight, or (far more commonly, since a single
-- broadcast attempt is not long-running) it crashed between the intent
-- write and the result write. Either way that row is read back as
-- AMBIGUOUS by `reconcile_quarantine_on_restart`, never silently dropped.
-- No foreign key to rust_fee_cycles: a live broadcast attempt is not part
-- of the SeedOnce/RehydratePerCycle shadow-commit pipeline that table
-- family serves, so `cycle_id` here is a plain nullable identity column.
CREATE TABLE IF NOT EXISTS rust_broadcast_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle_id TEXT,
    channel_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    method TEXT NOT NULL,
    params_json TEXT NOT NULL,
    submitted_at INTEGER NOT NULL,
    outcome TEXT CHECK (outcome IN ('success', 'rejected', 'clean_failure', 'ambiguous')),
    outcome_detail TEXT,
    completed_at INTEGER,
    UNIQUE (request_id)
);
CREATE INDEX IF NOT EXISTS idx_rust_broadcast_attempts_outcome
    ON rust_broadcast_attempts(outcome);

CREATE TABLE IF NOT EXISTS rust_consumed_arm_nonces (
    nonce TEXT PRIMARY KEY,
    consumed_at INTEGER NOT NULL,
    source_commit TEXT NOT NULL,
    binary_sha256 TEXT NOT NULL,
    arm_expires_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rust_rebalance_attempts (
    id INTEGER PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    source_channel TEXT NOT NULL,
    dest_channel TEXT NOT NULL,
    amount_sats INTEGER NOT NULL,
    max_fee_sats INTEGER NOT NULL,
    payment_hash TEXT,
    trigger TEXT NOT NULL CHECK (trigger IN ('cycle','manual','manual_force')),
    submitted_at INTEGER NOT NULL,
    outcome TEXT CHECK (outcome IN
        ('success','rejected','clean_failure_before_write','outcome_unknown')),
    outcome_detail TEXT,
    fee_paid_sats INTEGER,
    completed_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_rust_rebalance_attempts_outcome
    ON rust_rebalance_attempts(outcome);

CREATE TABLE IF NOT EXISTS rust_rebalance_reservations (
    id INTEGER PRIMARY KEY,
    attempt_request_id TEXT NOT NULL,
    reserved_sats INTEGER NOT NULL,
    reserved_at INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active','settled','released','quarantined')),
    settled_fee_sats INTEGER,
    resolved_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_rust_rebalance_reservations_status
    ON rust_rebalance_reservations(status);

CREATE TABLE IF NOT EXISTS rust_capital_intents (
    id INTEGER PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('open','close','defib')),
    peer_id TEXT NOT NULL,
    channel_id TEXT,
    amount_sats INTEGER NOT NULL,
    reason TEXT,
    submitted_at INTEGER NOT NULL,
    outcome TEXT CHECK (outcome IN
        ('clean_refusal','rejected','success','outcome_unknown')),
    outcome_detail TEXT,
    txid TEXT,
    completed_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_rust_capital_intents_outcome
    ON rust_capital_intents(outcome);

CREATE TABLE IF NOT EXISTS rust_capital_reservations (
    id INTEGER PRIMARY KEY,
    attempt_request_id TEXT NOT NULL,
    reserved_sats INTEGER NOT NULL,
    reserved_at INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active','settled','released','quarantined')),
    settled_sats INTEGER,
    resolved_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_rust_capital_reservations_status
    ON rust_capital_reservations(status);
";

// ---------------------------------------------------------------------------
// typed rows
// ---------------------------------------------------------------------------

/// One channel's persisted `v2_state_json` envelope at the current
/// generation. `v2_state_json` is opaque to this crate -- the caller
/// (`revops`/`revops-fees`) builds it the same way `JournalStateSink`
/// does today (`fee_state.rs::state_envelope` +
/// `revops_fees::pyjson::dumps_python`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeeStateRow {
    pub channel_id: String,
    pub v2_state_json: String,
    pub last_update: i64,
}

/// One would-broadcast prepared fee action
/// (`revops_fees::execution::PreparedFeeAction`), captured for audit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedFeeActionRow {
    pub channel_id: String,
    pub idempotency_key: Option<String>,
    pub old_fee_ppm: i64,
    pub new_fee_ppm: i64,
    pub feebase_msat: i64,
    pub htlcmin_msat: Option<i64>,
    pub htlcmax_msat: Option<i64>,
    pub message: String,
    pub at: i64,
}

/// One governor authorization decision
/// (`revops_fees::execution::GovernedTrace`), stored in `rust_fee_ledger`
/// with `kind = 'governor'`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GovernorAuditRow {
    pub channel_id: String,
    pub authorized: bool,
    pub reason_code: String,
    pub intent_id: String,
    pub idempotency_key: String,
    pub at: i64,
}

/// One `EconLedger::append` event, stored in `rust_fee_ledger` with `kind
/// = 'ledger'`. `details_json` is opaque to this crate (already
/// serialized by the caller, matching production's `python_dumps_default`
/// contract).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerAuditRow {
    pub channel_id: String,
    pub event_type: String,
    pub intent_id: String,
    pub idempotency_key: String,
    pub snapshot_id: String,
    pub at: i64,
    pub details_json: String,
}

/// One (cycle, channel) shadow decision's TERMINAL outcome -- broadcasts
/// and skips alike, unlike [`PreparedFeeActionRow`] which only exists for
/// would-broadcast decisions.
///
/// This is the exact column contract the 2026-07-26 revision plan's Task
/// R7 (`engagement_gate.py --source sqlite`) and Task R8 amendment #1
/// bind: `cycle_ts INTEGER`, `channel_id TEXT`, `would_broadcast
/// INTEGER`, `has_algorithm_values INTEGER`, `disposition TEXT`,
/// `skip_gate_comparable INTEGER`. `disposition` is stored VERBATIM as
/// `FeeDecision.trace`'s `"disposition"` entry emits it (`waiting_window`,
/// `sleeping_hold`, `alpha_guard`, `gossip_suppressed`, `broadcast`,
/// `gossip_refresh`, and any other value the trace emits -- never
/// re-encoded); `None` when the trace carries no disposition entry.
/// `skip_gate_comparable` mirrors the trace's `skip_gate_comparable`
/// marker: `true` (1) unless the trace explicitly sets it `false`
/// (P4b-T8b bootstrap/first-appearance channels).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowCycleOutcomeRow {
    pub cycle_ts: i64,
    pub channel_id: String,
    pub would_broadcast: bool,
    pub has_algorithm_values: bool,
    pub disposition: Option<String>,
    pub skip_gate_comparable: bool,
}

/// One mempool fee-rate sample, Rust's own recorder cadence.
#[derive(Debug, Clone, PartialEq)]
pub struct MempoolSampleRow {
    pub sampled_at: i64,
    pub sat_per_vbyte: f64,
}

/// One shadow-window comparison of Rust's 24h mempool moving average
/// against Python's (fix round 1, review finding 1; R8 binding
/// constraint: "during shadow, compare the Rust 24h MA against Python's
/// and record the comparison"). `python_ma` (and `delta`, always
/// `rust_ma - python_ma` when both are present) are `NULL` when Python's
/// MA could not be read this cycle (`FeeEvidence::mempool_ma_24h`'s `Err`
/// case) -- the row is recorded regardless, so absence is itself
/// evidence rather than a silently skipped comparison.
#[derive(Debug, Clone, PartialEq)]
pub struct MempoolMaComparisonRow {
    pub at: i64,
    pub cycle_ts: i64,
    pub rust_ma: f64,
    pub python_ma: Option<f64>,
    pub delta: Option<f64>,
}

/// One trigger receipt / coalescing decision.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FeeTriggerEventRow {
    pub trigger_type: String,
    pub channel_id: Option<String>,
    /// The cycle this trigger produced/joined, if any (R8 amendment item
    /// 3: trigger receipts share the cycle-ts keying the engagement gate
    /// uses).
    pub cycle_id: Option<String>,
    pub cycle_ts: Option<i64>,
    pub received_at: i64,
    pub coalesced: bool,
    pub detail: Option<String>,
}

/// A new quarantine entry to record (`rust_execution_quarantine`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineEntry {
    pub reason: String,
    pub cycle_id: Option<String>,
    pub channel_id: Option<String>,
    pub request_id: Option<String>,
    pub entered_at: i64,
}

/// A persisted quarantine row, as read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineRow {
    pub id: i64,
    pub reason: String,
    pub cycle_id: Option<String>,
    pub channel_id: Option<String>,
    pub request_id: Option<String>,
    pub entered_at: i64,
}

/// One SeedOnce seed event (Task 5 / revision-plan Task R6): either the
/// ONE successful cold-start import from Python's `fee_strategy_state`
/// (`outcome = 'seeded'`, provenance columns filled) or a fail-closed
/// refusal (`outcome = 'seed_refused'`, plus the offending channel +
/// field). There is deliberately no third outcome: a seed either imports
/// EVERY row through the exact `from_dict` parity path or refuses the
/// WHOLE snapshot -- no partial seed, no silent fresh-state fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeeSeedEventRow {
    pub seeded_at: i64,
    /// `'seeded'` or `'seed_refused'` (CHECK-enforced).
    pub outcome: String,
    /// The production DB path the snapshot was READ from (read-only).
    pub source_db_path: String,
    /// `MAX(last_update)` over the snapshot rows at seed time (0 if none).
    pub source_max_last_update: i64,
    /// Snapshot row count at seed time.
    pub row_count: i64,
    /// sha256 (hex) of the serialized seed payload (caller-defined
    /// canonical serialization; opaque to this crate).
    pub payload_sha256: String,
    /// The binary's source commit identity.
    pub source_commit: String,
    /// `seed_refused` only: the channel whose state Python's own
    /// `from_dict` would raise on.
    pub refused_channel: Option<String>,
    /// `seed_refused` only: the offending field (dotted path).
    pub refused_field: Option<String>,
    pub detail: Option<String>,
}

/// One scheduler restart marker (Task 5 step 4): process identity, the
/// generation found in the Rust-owned store at startup, which hydration
/// source was used, and the startup timestamp -- divergence evidence
/// readable without ever touching Python state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeeRestartMarkerRow {
    pub started_at: i64,
    pub process_id: i64,
    pub prior_generation: i64,
    /// `"python_seed"` or `"rust_generation:<n>"`.
    pub hydration_source: String,
    pub source_commit: String,
}

/// One runway timer snapshot / report identity row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunwaySnapshotRow {
    pub snapshot_at: i64,
    pub report_schema_version: String,
    pub source_commit: String,
    pub binary_sha256: String,
    pub summary_json: String,
}

/// One atomic fee-cycle commit: everything ONE dry-run cycle produces,
/// persisted together or not at all. `outcomes` is an addition beyond the
/// brief's illustrative snippet (see this module's doc comment on
/// `rust_fee_shadow_outcomes`); every other field matches the brief
/// verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FeeCycleCommit {
    pub cycle_id: String,
    pub started_at: i64,
    pub completed_at: i64,
    pub source_commit: String,
    pub binary_sha256: String,
    pub state_rows: Vec<FeeStateRow>,
    pub requests: Vec<PreparedFeeActionRow>,
    pub governor: Vec<GovernorAuditRow>,
    pub ledger: Vec<LedgerAuditRow>,
    pub outcomes: Vec<ShadowCycleOutcomeRow>,
    /// Task 42: successful seed provenance, committed ATOMICALLY with the
    /// generation-1 transaction or not at all. `Some` is only legal when
    /// this commit creates generation 1 from a virgin store (enforced by
    /// [`insert_successful_seed_locked`] inside the transaction — any
    /// other prior generation rolls the whole commit back). Scheduled
    /// non-bootstrap commits and every A3 commit leave this `None`.
    /// Refusals never appear here: they are standalone terminal facts
    /// ([`record_seed_refusal`]).
    pub pending_seed: Option<FeeSeedEventRow>,
    /// Task 44 / A3: the ONE trigger receipt this commit's decision is
    /// bound to, when this commit is an out-of-cycle
    /// `commit_initial_fee_event` rather than a scheduled cycle. Inserted
    /// inside the SAME transaction as every other row below -- unlike
    /// [`super::super::fee_state`]'s (crate `revops`) separate, NON-atomic
    /// `record_trigger_event`, a receipt claiming an effect must never be
    /// visible without its state/action/outcome, and vice versa (contract
    /// §3.2: "complete state, action, outcome, and receipt become visible
    /// atomically"). Regular per-cycle commits leave this `None` -- their
    /// trigger receipt (the `FixedInterval` dispatch) is written separately
    /// by the existing non-atomic path, which is fine there because a
    /// scheduled cycle's receipt never claims a specific effect the way an
    /// out-of-cycle one does.
    pub trigger_receipt: Option<FeeTriggerEventRow>,
}

/// The latest persisted controller state: the generation it was written
/// at, plus every channel's current row.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FeeStateSnapshot {
    pub generation: u64,
    pub rows: Vec<FeeStateRow>,
}

// ---------------------------------------------------------------------------
// atomic commit
// ---------------------------------------------------------------------------

/// Persist one complete fee cycle atomically: bump the state generation,
/// upsert every channel's state row at that generation, and insert the
/// cycle/request/ledger/outcome audit rows -- all inside ONE `BEGIN
/// IMMEDIATE` transaction. Any failure (duplicate request identity,
/// constraint violation, I/O error) rolls the ENTIRE transaction back:
/// the previous generation, and every row committed by a prior cycle,
/// are left byte-for-byte unchanged. Returns the new generation on
/// success.
pub fn commit_fee_cycle(conn: &Connection, commit: &FeeCycleCommit) -> Result<u64> {
    conn.execute_batch("BEGIN IMMEDIATE")
        .context("begin fee cycle commit transaction")?;
    let result = commit_fee_cycle_locked(conn, commit);
    if result.is_err() {
        // Best-effort ROLLBACK (mirrors budget.rs's `rollback_quietly`):
        // an error here means the transaction is already gone (e.g. the
        // connection itself failed), which the original error already
        // reports.
        let _ = conn.execute_batch("ROLLBACK");
    }
    result
}

fn commit_fee_cycle_locked(conn: &Connection, commit: &FeeCycleCommit) -> Result<u64> {
    let current_generation: i64 = conn
        .query_row(
            "SELECT generation FROM rust_fee_state_generation WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .optional()
        .context("read current fee state generation")?
        .unwrap_or(0);
    let next_generation = current_generation + 1;

    conn.execute(
        "INSERT INTO rust_fee_cycles
             (cycle_id, generation, started_at, completed_at, source_commit, binary_sha256, request_count)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            commit.cycle_id,
            next_generation,
            commit.started_at,
            commit.completed_at,
            commit.source_commit,
            commit.binary_sha256,
            commit.requests.len() as i64,
        ],
    )
    .context("insert rust_fee_cycles row")?;

    for row in &commit.state_rows {
        conn.execute(
            "INSERT INTO rust_fee_state (channel_id, generation, v2_state_json, last_update)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(channel_id) DO UPDATE SET
                 generation = excluded.generation,
                 v2_state_json = excluded.v2_state_json,
                 last_update = excluded.last_update",
            params![
                row.channel_id,
                next_generation,
                row.v2_state_json,
                row.last_update
            ],
        )
        .context("upsert rust_fee_state row")?;
    }

    for row in &commit.requests {
        conn.execute(
            "INSERT INTO rust_fee_requests
                 (cycle_id, channel_id, idempotency_key, old_fee_ppm, new_fee_ppm,
                  feebase_msat, htlcmin_msat, htlcmax_msat, message, at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                commit.cycle_id,
                row.channel_id,
                row.idempotency_key,
                row.old_fee_ppm,
                row.new_fee_ppm,
                row.feebase_msat,
                row.htlcmin_msat,
                row.htlcmax_msat,
                row.message,
                row.at,
            ],
        )
        .context("insert rust_fee_requests row")?;
    }

    for row in &commit.governor {
        conn.execute(
            "INSERT INTO rust_fee_ledger
                 (cycle_id, channel_id, kind, event_type, authorized, reason_code,
                  intent_id, idempotency_key, snapshot_id, at, details_json)
             VALUES (?1, ?2, 'governor', 'governor_decision', ?3, ?4, ?5, ?6, NULL, ?7, NULL)",
            params![
                commit.cycle_id,
                row.channel_id,
                row.authorized,
                row.reason_code,
                row.intent_id,
                row.idempotency_key,
                row.at,
            ],
        )
        .context("insert governor audit row")?;
    }

    for row in &commit.ledger {
        conn.execute(
            "INSERT INTO rust_fee_ledger
                 (cycle_id, channel_id, kind, event_type, authorized, reason_code,
                  intent_id, idempotency_key, snapshot_id, at, details_json)
             VALUES (?1, ?2, 'ledger', ?3, NULL, NULL, ?4, ?5, ?6, ?7, ?8)",
            params![
                commit.cycle_id,
                row.channel_id,
                row.event_type,
                row.intent_id,
                row.idempotency_key,
                row.snapshot_id,
                row.at,
                row.details_json,
            ],
        )
        .context("insert ledger audit row")?;
    }

    for row in &commit.outcomes {
        conn.execute(
            "INSERT INTO rust_fee_shadow_outcomes
                 (cycle_id, cycle_ts, channel_id, would_broadcast, has_algorithm_values,
                  disposition, skip_gate_comparable)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                commit.cycle_id,
                row.cycle_ts,
                row.channel_id,
                row.would_broadcast,
                row.has_algorithm_values,
                row.disposition,
                row.skip_gate_comparable,
            ],
        )
        .context("insert shadow outcome row")?;
    }

    conn.execute(
        "INSERT INTO rust_fee_state_generation (id, generation, updated_at)
         VALUES (1, ?1, ?2)
         ON CONFLICT(id) DO UPDATE SET
             generation = excluded.generation,
             updated_at = excluded.updated_at",
        params![next_generation, commit.completed_at],
    )
    .context("bump fee state generation")?;

    // Task 44 / A3: the trigger receipt, INSIDE this same transaction --
    // see `FeeCycleCommit::trigger_receipt`'s doc comment. A commit that
    // fails after this point rolls the receipt back along with everything
    // else; one that never reaches `COMMIT` never left a receipt claiming
    // an effect that did not durably happen.
    if let Some(receipt) = &commit.trigger_receipt {
        conn.execute(
            "INSERT INTO rust_fee_trigger_events
                 (trigger_type, channel_id, cycle_id, cycle_ts, received_at, coalesced, detail)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                receipt.trigger_type,
                receipt.channel_id,
                commit.cycle_id,
                receipt.cycle_ts,
                receipt.received_at,
                receipt.coalesced,
                receipt.detail,
            ],
        )
        .context("insert atomic trigger receipt row")?;
    }

    // Task 42: successful seed provenance rides the SAME transaction as
    // the generation bump it describes -- see
    // `FeeCycleCommit::pending_seed` and `insert_successful_seed_locked`
    // (which enforces the virgin-store / generation-1 gate).
    if let Some(seed) = &commit.pending_seed {
        insert_successful_seed_locked(
            conn,
            seed,
            current_generation,
            &commit.cycle_id,
            next_generation,
        )?;
    }

    conn.execute_batch("COMMIT")
        .context("commit fee cycle transaction")?;
    Ok(next_generation as u64)
}

/// Load the current generation and every channel's state row at that
/// generation. `generation == 0` with empty `rows` means no cycle has
/// ever committed (cold start: the caller must `SeedOnce` from the
/// read-only Python snapshot -- Task 5).
pub fn load_latest_state(conn: &Connection) -> Result<FeeStateSnapshot> {
    let generation: i64 = conn
        .query_row(
            "SELECT generation FROM rust_fee_state_generation WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .optional()
        .context("read fee state generation")?
        .unwrap_or(0);
    let mut stmt = conn
        .prepare(
            "SELECT channel_id, v2_state_json, last_update FROM rust_fee_state ORDER BY channel_id",
        )
        .context("prepare load_latest_state query")?;
    let rows = stmt
        .query_map([], |r| {
            Ok(FeeStateRow {
                channel_id: r.get(0)?,
                v2_state_json: r.get(1)?,
                last_update: r.get(2)?,
            })
        })
        .context("run load_latest_state query")?
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("collect load_latest_state rows")?;
    Ok(FeeStateSnapshot {
        generation: generation.max(0) as u64,
        rows,
    })
}

/// Scalar-only read of the current state generation (fix round 1, I-5):
/// [`load_latest_state`] materialises every channel's `v2_state_json` row
/// just to answer "what generation is this store at" -- callers that only
/// need the number (the runway status RPC) use this instead, so a request
/// that only wants one integer doesn't force a full-table read on the
/// single-owner actor the cycle loop writes through (head-of-line
/// blocking risk on a busy store).
/// Task 44 / A3, live-review finding F3: has a commit with this EXACT
/// (stable, content-derived) `cycle_id` already been durably committed?
/// The out-of-cycle new-channel commit path uses this to distinguish a
/// genuinely new event from a replay of the SAME event after a restart
/// (the replayed event recomputes an IDENTICAL `cycle_id`, since it is
/// derived only from the event's own content -- channel id, transition,
/// and event timestamp -- never wall-clock-at-processing or a process id).
pub fn cycle_exists(conn: &Connection, cycle_id: &str) -> Result<bool> {
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM rust_fee_cycles WHERE cycle_id = ?1",
            params![cycle_id],
            |r| r.get(0),
        )
        .context("check cycle_id existence")?;
    Ok(count > 0)
}

/// Task 44 / A3 (live-review F7): [`cycle_exists`] plus the CURRENT state
/// generation, read together inside one transaction so the pair is a
/// consistent point-in-time observation. The generation lets the
/// scheduler bind its later commit to the exact state it decided against
/// (via [`commit_fee_cycle_guarded`]'s compare-and-set) -- this read
/// alone is advisory; the CAS is the guard.
pub fn cycle_exists_with_generation(conn: &Connection, cycle_id: &str) -> Result<(bool, u64)> {
    conn.execute_batch("BEGIN")
        .context("begin cycle_exists_with_generation read transaction")?;
    let result = (|| {
        let exists = cycle_exists(conn, cycle_id)?;
        let generation = current_state_generation(conn)?;
        Ok((exists, generation))
    })();
    let _ = conn.execute_batch("COMMIT");
    result
}

/// Task 44 / A3 (live-review F7): the outcome of a guarded
/// (compare-and-set) commit -- either the commit landed (with its new
/// generation), or the store's generation no longer matched what the
/// decision was computed against and NOTHING was written. A conflict is
/// an in-band, expected-shape outcome (the caller fails closed and
/// discards its staged state), distinct from an `Err` (the commit could
/// not even be attempted/completed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardedCommitOutcome {
    Committed(u64),
    GenerationConflict { expected: u64, found: u64 },
}

/// [`commit_fee_cycle`]'s guarded form (Task 44 / A3, live-review F7):
/// inside the SAME `BEGIN IMMEDIATE` transaction, first compare the
/// current state generation against `expected_prior_generation`; on
/// mismatch roll back having written NOTHING and report the conflict
/// in-band. The out-of-cycle A3 commit uses this so a decision computed
/// against state generation N can never land on top of a store that has
/// since advanced past N.
pub fn commit_fee_cycle_guarded(
    conn: &Connection,
    commit: &FeeCycleCommit,
    expected_prior_generation: u64,
) -> Result<GuardedCommitOutcome> {
    conn.execute_batch("BEGIN IMMEDIATE")
        .context("begin guarded fee cycle commit transaction")?;
    let found = match current_state_generation(conn) {
        Ok(g) => g,
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e);
        }
    };
    if found != expected_prior_generation {
        let _ = conn.execute_batch("ROLLBACK");
        return Ok(GuardedCommitOutcome::GenerationConflict {
            expected: expected_prior_generation,
            found,
        });
    }
    let result = commit_fee_cycle_locked(conn, commit);
    match result {
        Ok(generation) => Ok(GuardedCommitOutcome::Committed(generation)),
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

pub fn current_state_generation(conn: &Connection) -> Result<u64> {
    let generation: i64 = conn
        .query_row(
            "SELECT generation FROM rust_fee_state_generation WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .optional()
        .context("read fee state generation")?
        .unwrap_or(0);
    Ok(generation.max(0) as u64)
}

// ---------------------------------------------------------------------------
// mempool samples
// ---------------------------------------------------------------------------

/// Record one mempool fee-rate sample.
pub fn record_mempool_sample(conn: &Connection, sampled_at: i64, sat_per_vbyte: f64) -> Result<()> {
    conn.execute(
        "INSERT INTO rust_mempool_fee_history (sampled_at, sat_per_vbyte) VALUES (?1, ?2)",
        params![sampled_at, sat_per_vbyte],
    )
    .context("record mempool sample")?;
    Ok(())
}

/// Delete every sample strictly BEFORE `cutoff`. Returns the row count
/// removed. Used standalone by tests; production always goes through
/// [`record_mempool_sample_pruned`]'s single transaction (stateful-shadow
/// plan Task 6 step 1: "old rows are pruned transactionally").
pub fn prune_mempool_samples_before(conn: &Connection, cutoff: i64) -> Result<usize> {
    let n = conn
        .execute(
            "DELETE FROM rust_mempool_fee_history WHERE sampled_at < ?1",
            params![cutoff],
        )
        .context("prune mempool samples")?;
    Ok(n)
}

/// Insert one sample and prune everything strictly before `retain_since`,
/// atomically: both the `INSERT` and the `DELETE` run inside ONE `BEGIN
/// IMMEDIATE`/`COMMIT` (mirroring [`commit_fee_cycle`]'s rollback-on-error
/// shape), so a failure in either step leaves the table exactly as it was
/// before the call -- Task 6 step 1's "old rows are pruned transactionally"
/// contract. This is Rust's OWN recorder cadence (never Python's table);
/// callers gate the call the same way Python gates `record_mempool_fee`
/// (only when Vegas Reflex is enabled and chain costs resolved this
/// cycle), so the two histories stay sample-for-sample comparable.
/// The post-refresh Rust-only mempool window aggregate (Task 42):
/// everything a virgin `SeedOnce` decision needs, without materializing
/// the window's rows on the single DB actor. `average` is `None` iff
/// `count == 0`.
#[derive(Debug, Clone, PartialEq)]
pub struct MempoolWindow {
    pub count: u64,
    pub latest_sampled_at: Option<i64>,
    pub average: Option<f64>,
}

/// Task 42: insert the CURRENT Rust sample, prune rows strictly before
/// `retain_since`, and read back the resulting Rust-only window
/// aggregate — all inside ONE `BEGIN IMMEDIATE`/`COMMIT`, so a virgin
/// first cycle's frozen evidence includes its own current observation
/// (the two-cycle bootstrap defect) and a failure anywhere leaves the
/// table exactly as it was. The committed sample deliberately survives a
/// LATER failed cycle: it is a truthful observation, never a
/// state-success claim.
pub fn refresh_mempool_window(
    conn: &Connection,
    sampled_at: i64,
    sat_per_vbyte: f64,
    retain_since: i64,
) -> Result<MempoolWindow> {
    conn.execute_batch("BEGIN IMMEDIATE")
        .context("begin mempool window refresh transaction")?;
    let result = (|| -> Result<MempoolWindow> {
        record_mempool_sample(conn, sampled_at, sat_per_vbyte)?;
        prune_mempool_samples_before(conn, retain_since)?;
        let window = conn
            .query_row(
                "SELECT COUNT(*), MAX(sampled_at), AVG(sat_per_vbyte)
                 FROM rust_mempool_fee_history",
                [],
                |r| {
                    Ok(MempoolWindow {
                        count: r.get::<_, i64>(0)? as u64,
                        latest_sampled_at: r.get(1)?,
                        average: r.get(2)?,
                    })
                },
            )
            .context("read refreshed mempool window aggregate")?;
        // COMMIT is INSIDE the guarded result (Task 42 correction F3): a
        // COMMIT error (e.g. SQLITE_BUSY from a rollback-journal reader)
        // can leave the transaction OPEN with the inserted sample visible
        // to later commands on this connection -- it must hit the same
        // ROLLBACK path as any other failure.
        conn.execute_batch("COMMIT")
            .context("commit mempool window refresh transaction")?;
        Ok(window)
    })();
    if result.is_err() {
        let _ = conn.execute_batch("ROLLBACK");
    }
    result
}

pub fn record_mempool_sample_pruned(
    conn: &Connection,
    sampled_at: i64,
    sat_per_vbyte: f64,
    retain_since: i64,
) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE")
        .context("begin mempool sample transaction")?;
    let result = (|| -> Result<()> {
        record_mempool_sample(conn, sampled_at, sat_per_vbyte)?;
        prune_mempool_samples_before(conn, retain_since)?;
        // COMMIT inside the guarded result -- Task 42 correction F3's
        // defect class (a failed COMMIT must reach ROLLBACK too).
        conn.execute_batch("COMMIT")
            .context("commit mempool sample transaction")
    })();
    if result.is_err() {
        let _ = conn.execute_batch("ROLLBACK");
    }
    result
}

/// Every sample at or after `since`, oldest first -- the input a 24-hour
/// moving average is computed from.
pub fn query_mempool_samples_since(conn: &Connection, since: i64) -> Result<Vec<MempoolSampleRow>> {
    let mut stmt = conn
        .prepare(
            "SELECT sampled_at, sat_per_vbyte FROM rust_mempool_fee_history
             WHERE sampled_at >= ?1 ORDER BY sampled_at",
        )
        .context("prepare mempool samples query")?;
    let rows = stmt
        .query_map(params![since], |r| {
            Ok(MempoolSampleRow {
                sampled_at: r.get(0)?,
                sat_per_vbyte: r.get(1)?,
            })
        })
        .context("run mempool samples query")?
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("collect mempool samples")?;
    Ok(rows)
}

/// The count of, and the most recent timestamp among, every sample at or
/// after `since` -- one aggregate-only query, never the rows themselves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MempoolSampleStats {
    pub count: i64,
    pub latest_sampled_at: Option<i64>,
}

/// Scalar-only sibling of [`query_mempool_samples_since`] (fix round 1,
/// I-5): a caller that only needs freshness bookkeeping (sample count,
/// latest timestamp -- the runway status RPC's use case) must not force
/// materialising every row in the window on the single-owner actor the
/// cycle loop writes through.
pub fn mempool_sample_stats(conn: &Connection, since: i64) -> Result<MempoolSampleStats> {
    conn.query_row(
        "SELECT COUNT(*), MAX(sampled_at) FROM rust_mempool_fee_history WHERE sampled_at >= ?1",
        params![since],
        |r| {
            Ok(MempoolSampleStats {
                count: r.get(0)?,
                latest_sampled_at: r.get(1)?,
            })
        },
    )
    .context("read mempool sample stats")
}

/// Persist one shadow-window mempool 24h-MA comparison (fix round 1,
/// review finding 1). Returns the new row's `rowid`. `python_ma`/`delta`
/// are recorded as-given, `NULL` included -- the caller decides absence,
/// this function never substitutes a value.
pub fn record_mempool_ma_comparison(
    conn: &Connection,
    row: &MempoolMaComparisonRow,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO rust_mempool_ma_comparison (at, cycle_ts, rust_ma, python_ma, delta)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![row.at, row.cycle_ts, row.rust_ma, row.python_ma, row.delta],
    )
    .context("record mempool MA comparison")?;
    Ok(conn.last_insert_rowid())
}

/// Every mempool MA comparison recorded at or after `since`, oldest
/// first -- the daily rollup's read path (and this crate's own tests).
pub fn query_mempool_ma_comparisons_since(
    conn: &Connection,
    since: i64,
) -> Result<Vec<MempoolMaComparisonRow>> {
    let mut stmt = conn
        .prepare(
            "SELECT at, cycle_ts, rust_ma, python_ma, delta FROM rust_mempool_ma_comparison
             WHERE at >= ?1 ORDER BY at",
        )
        .context("prepare mempool MA comparison query")?;
    let rows = stmt
        .query_map(params![since], |r| {
            Ok(MempoolMaComparisonRow {
                at: r.get(0)?,
                cycle_ts: r.get(1)?,
                rust_ma: r.get(2)?,
                python_ma: r.get(3)?,
                delta: r.get(4)?,
            })
        })
        .context("run mempool MA comparison query")?
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("collect mempool MA comparisons")?;
    Ok(rows)
}

// ---------------------------------------------------------------------------
// trigger events
// ---------------------------------------------------------------------------

/// Record one trigger receipt / coalescing decision.
pub fn record_trigger_event(conn: &Connection, ev: &FeeTriggerEventRow) -> Result<()> {
    conn.execute(
        "INSERT INTO rust_fee_trigger_events
             (trigger_type, channel_id, cycle_id, cycle_ts, received_at, coalesced, detail)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            ev.trigger_type,
            ev.channel_id,
            ev.cycle_id,
            ev.cycle_ts,
            ev.received_at,
            ev.coalesced,
            ev.detail,
        ],
    )
    .context("record fee trigger event")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// mutation count
// ---------------------------------------------------------------------------

/// Total number of would-broadcast prepared fee actions ever committed.
/// Used at rehearsal/dry-run checkpoints to confirm the mutation count is
/// stable across a window where Rust must not be mutating anything (the
/// design spec's "confirm Rust is observer/dry-run and mutation count is
/// stable" rehearsal step) -- a real live broadcast is a strictly later,
/// separately-gated capability that does not exist yet.
pub fn mutation_count(conn: &Connection) -> Result<i64> {
    conn.query_row("SELECT COUNT(*) FROM rust_fee_requests", [], |r| r.get(0))
        .context("read fee mutation count")
}

/// Total number of live broadcast attempts ever recorded (Task 9's
/// `rust_broadcast_attempts` ledger -- one row per would-broadcast request
/// that actually reached
/// [`crate::owner::ObserverHandle::insert_broadcast_attempt`], i.e. a
/// genuine live dispatch attempt by the `revops` crate's guarded
/// broadcaster, success/rejected/clean-failure/ambiguous alike). Distinct
/// from [`mutation_count`], which counts SHADOW-mode would-broadcast
/// preparations that were never actually sent -- Task 10's runway status
/// RPC reports both as separate counters (prepared-request count vs.
/// mutation-call count).
pub fn broadcast_attempt_count(conn: &Connection) -> Result<i64> {
    conn.query_row("SELECT COUNT(*) FROM rust_broadcast_attempts", [], |r| {
        r.get(0)
    })
    .context("read broadcast attempt count")
}

// ---------------------------------------------------------------------------
// quarantine
// ---------------------------------------------------------------------------

/// Record a new quarantine entry. Returns the new row's id.
pub fn insert_quarantine(conn: &Connection, entry: &QuarantineEntry) -> Result<i64> {
    conn.execute(
        "INSERT INTO rust_execution_quarantine
             (entered_at, reason, cycle_id, channel_id, request_id, cleared_at)
         VALUES (?1, ?2, ?3, ?4, ?5, NULL)",
        params![
            entry.entered_at,
            entry.reason,
            entry.cycle_id,
            entry.channel_id,
            entry.request_id,
        ],
    )
    .context("insert execution quarantine row")?;
    Ok(conn.last_insert_rowid())
}

/// The most recent still-active (never cleared) quarantine entry, if any.
/// `Some(_)` means execution must stay denied (fail closed): an ambiguous
/// post-submission transport outcome is never retried automatically.
pub fn active_quarantine(conn: &Connection) -> Result<Option<QuarantineRow>> {
    conn.query_row(
        "SELECT id, reason, cycle_id, channel_id, request_id, entered_at
         FROM rust_execution_quarantine
         WHERE cleared_at IS NULL
         ORDER BY entered_at DESC, id DESC
         LIMIT 1",
        [],
        |r| {
            Ok(QuarantineRow {
                id: r.get(0)?,
                reason: r.get(1)?,
                cycle_id: r.get(2)?,
                channel_id: r.get(3)?,
                request_id: r.get(4)?,
                entered_at: r.get(5)?,
            })
        },
    )
    .optional()
    .context("read active execution quarantine")
}

// ---------------------------------------------------------------------------
// seed events + restart markers (Task 5)
// ---------------------------------------------------------------------------

/// Record one seed event (successful import or fail-closed refusal).
/// Returns the new row's id. The `outcome` CHECK rejects anything but
/// `'seeded'`/`'seed_refused'`.
/// Record one STANDALONE seed-refusal event (Task 42): refusal is itself
/// the terminal fact being recorded, so it stays durable on its own. A
/// row claiming SUCCESS (`outcome = 'seeded'`) is refused here outright —
/// successful seed provenance is only writable inside the same
/// transaction as the generation-1 commit
/// ([`insert_successful_seed_locked`], reachable exclusively through
/// [`FeeCycleCommit::pending_seed`]), so a "seeded" row can never exist
/// without the durable state it claims.
pub fn record_seed_refusal(conn: &Connection, ev: &FeeSeedEventRow) -> Result<i64> {
    anyhow::ensure!(
        ev.outcome == "seed_refused",
        "record_seed_refusal only records refusals (got outcome={:?}); successful \
         seed provenance commits atomically with generation 1 via \
         FeeCycleCommit::pending_seed",
        ev.outcome
    );
    insert_seed_event_row(conn, ev).context("insert seed refusal row")?;
    Ok(conn.last_insert_rowid())
}

/// The raw row insert shared by [`record_seed_refusal`] and
/// [`insert_successful_seed_locked`]. Private: every public path goes
/// through one of those two typed gates.
fn insert_seed_event_row(conn: &Connection, ev: &FeeSeedEventRow) -> rusqlite::Result<usize> {
    conn.execute(
        "INSERT INTO rust_fee_seed_events
             (seeded_at, outcome, source_db_path, source_max_last_update, row_count,
              payload_sha256, source_commit, refused_channel, refused_field, detail)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            ev.seeded_at,
            ev.outcome,
            ev.source_db_path,
            ev.source_max_last_update,
            ev.row_count,
            ev.payload_sha256,
            ev.source_commit,
            ev.refused_channel,
            ev.refused_field,
            ev.detail,
        ],
    )
}

/// Insert successful seed provenance INSIDE an already-open commit
/// transaction (Task 42). Callable only from
/// [`commit_fee_cycle_locked`]; the gates here are the DB-level belt
/// under the scheduler's own sequencing:
///
/// * the row must claim `outcome = 'seeded'` (refusals never ride a
///   commit — they are standalone terminal facts);
/// * the transaction must be creating generation 1 from a virgin store
///   (prior generation 0). Any other prior generation means state no
///   longer derives purely from the Python import this provenance
///   describes, so persisting it would be a false claim — the whole
///   transaction rolls back.
fn insert_successful_seed_locked(
    conn: &Connection,
    ev: &FeeSeedEventRow,
    prior_generation: i64,
    bound_cycle_id: &str,
    bound_generation: i64,
) -> Result<()> {
    anyhow::ensure!(
        ev.outcome == "seeded",
        "pending seed provenance must claim outcome='seeded' (got {:?})",
        ev.outcome
    );
    anyhow::ensure!(
        prior_generation == 0,
        "pending seed provenance requires a virgin store: prior generation is {} \
         (state no longer derives purely from the described Python import); \
         rolling the commit back",
        prior_generation
    );
    anyhow::ensure!(
        bound_generation == 1,
        "successful seed provenance binds to generation 1 exactly (got {bound_generation})"
    );
    conn.execute(
        "INSERT INTO rust_fee_seed_events
             (seeded_at, outcome, source_db_path, source_max_last_update, row_count,
              payload_sha256, source_commit, refused_channel, refused_field, detail,
              bound_cycle_id, bound_generation)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            ev.seeded_at,
            ev.outcome,
            ev.source_db_path,
            ev.source_max_last_update,
            ev.row_count,
            ev.payload_sha256,
            ev.source_commit,
            ev.refused_channel,
            ev.refused_field,
            ev.detail,
            bound_cycle_id,
            bound_generation,
        ],
    )
    .context("insert bound successful seed provenance row")?;
    Ok(())
}

/// The most recently recorded seed event, if any.
pub fn latest_seed_event(conn: &Connection) -> Result<Option<FeeSeedEventRow>> {
    conn.query_row(
        "SELECT seeded_at, outcome, source_db_path, source_max_last_update, row_count,
                payload_sha256, source_commit, refused_channel, refused_field, detail
         FROM rust_fee_seed_events
         ORDER BY id DESC
         LIMIT 1",
        [],
        |r| {
            Ok(FeeSeedEventRow {
                seeded_at: r.get(0)?,
                outcome: r.get(1)?,
                source_db_path: r.get(2)?,
                source_max_last_update: r.get(3)?,
                row_count: r.get(4)?,
                payload_sha256: r.get(5)?,
                source_commit: r.get(6)?,
                refused_channel: r.get(7)?,
                refused_field: r.get(8)?,
                detail: r.get(9)?,
            })
        },
    )
    .optional()
    .context("read latest fee seed event")
}

/// Task 42 correction F1: the DERIVED, verified seed-provenance state —
/// the ONLY representation authority decisions may consume (raw
/// generation/event fields can be misread as proof).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeedBindingState {
    /// Generation 0, no state rows, no successful seed row: seeding is
    /// legitimately still pending on the first cycle.
    VirginStore,
    /// Generation >= 1 and EXACTLY ONE `outcome='seeded'` row, bound to
    /// generation 1 and to a `rust_fee_cycles` row that exists at
    /// generation 1. Carries the bound cycle id.
    VerifiedBound { cycle_id: String },
    /// Everything else: refusal-only nonvirgin stores, legacy UNBOUND
    /// successful rows, duplicate/conflicting rows, a bound cycle that
    /// does not exist, generation >= 1 with no successful row (e.g. an
    /// out-of-cycle-first store), or rows on a virgin store. Fail-closed
    /// for autonomous-nonvirgin and live readiness; the reason is
    /// diagnostic, never authorizing.
    Invalid { reason: String },
}

/// Derive [`SeedBindingState`] from the store (Task 42 correction F1).
/// Read-only; every check is over durable rows, never in-memory state.
pub fn verified_seed_binding(conn: &Connection) -> Result<SeedBindingState> {
    let generation = current_state_generation(conn)?;
    let seeded: Vec<(i64, Option<String>, Option<i64>)> = conn
        .prepare(
            "SELECT id, bound_cycle_id, bound_generation FROM rust_fee_seed_events
             WHERE outcome = 'seeded' ORDER BY id",
        )
        .context("prepare successful-seed rows read")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .context("read successful-seed rows")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("collect successful-seed rows")?;

    if generation == 0 {
        return if seeded.is_empty() {
            Ok(SeedBindingState::VirginStore)
        } else {
            Ok(SeedBindingState::Invalid {
                reason: "successful seed row exists on a generation-0 store".to_string(),
            })
        };
    }
    // F-R4: refusals BEFORE the one atomic success are legitimate retry
    // history; ANY refusal recorded AFTER the successful bound row is
    // conflicting/corrupt provenance (the success no longer describes the
    // store's terminal seed fact) and must never verify.
    if let [(success_id, _, _)] = seeded.as_slice() {
        let later_refusals: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM rust_fee_seed_events
                 WHERE outcome = 'seed_refused' AND id > ?1",
                params![success_id],
                |r| r.get(0),
            )
            .context("count post-success refusal rows")?;
        if later_refusals > 0 {
            return Ok(SeedBindingState::Invalid {
                reason: format!(
                    "{later_refusals} refusal row(s) recorded AFTER the successful bound \
                     seed — conflicting provenance (refusals may only precede the one \
                     atomic success)"
                ),
            });
        }
    }
    let (bound_cycle_id, bound_generation) = match seeded.as_slice() {
        [] => {
            return Ok(SeedBindingState::Invalid {
                reason: format!(
                    "generation {generation} store has NO successful seed row (an \
                     out-of-cycle-first or corrupt store; never authority-eligible)"
                ),
            })
        }
        [(_, cycle, generation)] => (cycle.clone(), *generation),
        more => {
            return Ok(SeedBindingState::Invalid {
                reason: format!(
                    "{} successful seed rows exist; SeedOnce permits exactly one",
                    more.len()
                ),
            })
        }
    };
    let (Some(cycle_id), Some(bound_generation)) = (bound_cycle_id, bound_generation) else {
        return Ok(SeedBindingState::Invalid {
            reason: "successful seed row is UNBOUND (legacy standalone row; requires \
                     explicit verified reconciliation, never silent acceptance)"
                .to_string(),
        });
    };
    if bound_generation != 1 {
        return Ok(SeedBindingState::Invalid {
            reason: format!("successful seed row binds generation {bound_generation}, not 1"),
        });
    }
    let cycle_generation: Option<i64> = conn
        .query_row(
            "SELECT generation FROM rust_fee_cycles WHERE cycle_id = ?1",
            params![cycle_id],
            |r| r.get(0),
        )
        .optional()
        .context("read bound seed cycle row")?;
    match cycle_generation {
        Some(1) => Ok(SeedBindingState::VerifiedBound { cycle_id }),
        Some(other) => Ok(SeedBindingState::Invalid {
            reason: format!("seed-bound cycle {cycle_id} exists at generation {other}, not 1"),
        }),
        None => Ok(SeedBindingState::Invalid {
            reason: format!("seed-bound cycle {cycle_id} does not exist"),
        }),
    }
}

/// Record one restart marker. Returns the new row's id.
pub fn record_restart_marker(conn: &Connection, marker: &FeeRestartMarkerRow) -> Result<i64> {
    conn.execute(
        "INSERT INTO rust_fee_restart_markers
             (started_at, process_id, prior_generation, hydration_source, source_commit)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            marker.started_at,
            marker.process_id,
            marker.prior_generation,
            marker.hydration_source,
            marker.source_commit,
        ],
    )
    .context("insert fee restart marker row")?;
    Ok(conn.last_insert_rowid())
}

/// The most recently recorded restart marker, if any.
pub fn latest_restart_marker(conn: &Connection) -> Result<Option<FeeRestartMarkerRow>> {
    conn.query_row(
        "SELECT started_at, process_id, prior_generation, hydration_source, source_commit
         FROM rust_fee_restart_markers
         ORDER BY id DESC
         LIMIT 1",
        [],
        |r| {
            Ok(FeeRestartMarkerRow {
                started_at: r.get(0)?,
                process_id: r.get(1)?,
                prior_generation: r.get(2)?,
                hydration_source: r.get(3)?,
                source_commit: r.get(4)?,
            })
        },
    )
    .optional()
    .context("read latest fee restart marker")
}

// ---------------------------------------------------------------------------
// runway snapshots
// ---------------------------------------------------------------------------

/// Record one runway timer snapshot / report identity row.
pub fn record_runway_snapshot(conn: &Connection, snap: &RunwaySnapshotRow) -> Result<i64> {
    conn.execute(
        "INSERT INTO rust_runway_snapshots
             (snapshot_at, report_schema_version, source_commit, binary_sha256, summary_json)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            snap.snapshot_at,
            snap.report_schema_version,
            snap.source_commit,
            snap.binary_sha256,
            snap.summary_json,
        ],
    )
    .context("insert runway snapshot row")?;
    Ok(conn.last_insert_rowid())
}

// ---------------------------------------------------------------------------
// broadcast attempts + restart-quarantine reconciliation (Task 9)
// ---------------------------------------------------------------------------

/// One broadcast attempt's PRE-submission intent (stateful-shadow revision
/// plan, Task 9): written through the store BEFORE any socket write, so a
/// process crash between the intent write and the (never-written) result
/// leaves a row with `outcome IS NULL` -- readable as ambiguous by
/// [`unresolved_broadcast_attempts`], and restored into an active
/// quarantine by [`reconcile_quarantine_on_restart`] rather than silently
/// dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BroadcastAttemptIntent {
    /// The shadow/live cycle this request belongs to, if any. No FK to
    /// `rust_fee_cycles` -- see this module's DDL comment.
    pub cycle_id: Option<String>,
    pub channel_id: String,
    /// Unique request identity (e.g. the governor's idempotency key) --
    /// `UNIQUE(request_id)` so a retried submission of the exact same
    /// request cannot silently insert a second intent row.
    pub request_id: String,
    /// Always [`crate::owner`]'s caller-supplied RPC method name (in
    /// practice always the one guarded broadcast RPC method name -- kept
    /// as a column, not a constant, so this ledger stays a generic attempt
    /// log).
    pub method: String,
    /// The exact wire params sent, already serialized by the caller.
    pub params_json: String,
    pub submitted_at: i64,
}

/// The terminal classification of one broadcast attempt's transport
/// outcome. `Success`/`Rejected` are definite -- CLN told us plainly, one
/// way or the other. `CleanFailure` means no bytes could possibly have
/// reached lightningd (the attempt was refused before any write was even
/// attempted). `Ambiguous` means bytes may have been accepted by
/// lightningd and the true outcome is genuinely unknown -- this is the
/// ONLY terminal outcome that also quarantines execution (see
/// [`reconcile_quarantine_on_restart`] and `revops::fee_execution`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BroadcastAttemptOutcome {
    Success,
    Rejected,
    CleanFailure,
    Ambiguous,
}

impl BroadcastAttemptOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Rejected => "rejected",
            Self::CleanFailure => "clean_failure",
            Self::Ambiguous => "ambiguous",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "success" => Some(Self::Success),
            "rejected" => Some(Self::Rejected),
            "clean_failure" => Some(Self::CleanFailure),
            "ambiguous" => Some(Self::Ambiguous),
            _ => None,
        }
    }
}

/// A persisted broadcast-attempt row, as read back. `outcome.is_none()`
/// means no result was ever recorded for this intent (see
/// [`BroadcastAttemptIntent`]'s doc comment).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BroadcastAttemptRow {
    pub id: i64,
    pub cycle_id: Option<String>,
    pub channel_id: String,
    pub request_id: String,
    pub method: String,
    pub params_json: String,
    pub submitted_at: i64,
    pub outcome: Option<BroadcastAttemptOutcome>,
    pub outcome_detail: Option<String>,
    pub completed_at: Option<i64>,
}

/// Persist one broadcast attempt's intent BEFORE any socket write. Returns
/// the new row's id, which the caller must pass to
/// [`record_broadcast_attempt_result`] once the transport outcome is
/// known.
pub fn insert_broadcast_attempt(conn: &Connection, intent: &BroadcastAttemptIntent) -> Result<i64> {
    conn.execute(
        "INSERT INTO rust_broadcast_attempts
             (cycle_id, channel_id, request_id, method, params_json, submitted_at,
              outcome, outcome_detail, completed_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, NULL)",
        params![
            intent.cycle_id,
            intent.channel_id,
            intent.request_id,
            intent.method,
            intent.params_json,
            intent.submitted_at,
        ],
    )
    .context("insert broadcast attempt intent")?;
    Ok(conn.last_insert_rowid())
}

/// Record one broadcast attempt's terminal transport outcome, AFTER the
/// socket call returned (or was conclusively refused before any write).
/// Errors if `id` names no row (the intent must have been persisted
/// first).
pub fn record_broadcast_attempt_result(
    conn: &Connection,
    id: i64,
    outcome: BroadcastAttemptOutcome,
    outcome_detail: Option<&str>,
    completed_at: i64,
) -> Result<()> {
    let updated = conn
        .execute(
            "UPDATE rust_broadcast_attempts
             SET outcome = ?1, outcome_detail = ?2, completed_at = ?3
             WHERE id = ?4",
            params![outcome.as_str(), outcome_detail, completed_at, id],
        )
        .context("record broadcast attempt result")?;
    if updated == 0 {
        anyhow::bail!("no rust_broadcast_attempts row with id {id} (intent was never persisted?)");
    }
    Ok(())
}

/// Every broadcast attempt with no recorded result, oldest first -- the
/// exact set [`reconcile_quarantine_on_restart`] resolves at startup.
pub fn unresolved_broadcast_attempts(conn: &Connection) -> Result<Vec<BroadcastAttemptRow>> {
    let mut stmt = conn
        .prepare(
            "SELECT id, cycle_id, channel_id, request_id, method, params_json, submitted_at,
                    outcome, outcome_detail, completed_at
             FROM rust_broadcast_attempts
             WHERE outcome IS NULL
             ORDER BY id",
        )
        .context("prepare unresolved broadcast attempts query")?;
    let rows = stmt
        .query_map([], |r| {
            let outcome: Option<String> = r.get(7)?;
            Ok(BroadcastAttemptRow {
                id: r.get(0)?,
                cycle_id: r.get(1)?,
                channel_id: r.get(2)?,
                request_id: r.get(3)?,
                method: r.get(4)?,
                params_json: r.get(5)?,
                submitted_at: r.get(6)?,
                outcome: outcome.and_then(|s| BroadcastAttemptOutcome::from_str(&s)),
                outcome_detail: r.get(8)?,
                completed_at: r.get(9)?,
            })
        })
        .context("run unresolved broadcast attempts query")?
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("collect unresolved broadcast attempts")?;
    Ok(rows)
}

/// Restart reconciliation (stateful-shadow revision plan, Task 9 brief
/// Step 2): find every broadcast-attempt intent with no recorded result
/// (a process crash between the intent write and the result write -- see
/// [`BroadcastAttemptIntent`]'s doc comment), mark each one `Ambiguous` so
/// it is never re-scanned as unresolved again, and ensure an active
/// quarantine entry exists blocking the NEXT batch.
///
/// This is RESTORATION, never an automatic clear: an already-active
/// quarantine is left exactly as it was (this module offers no clear
/// function at all -- see the module doc's `rust_execution_quarantine`
/// bullet); a genuinely orphaned intent always produces (or confirms) an
/// active quarantine. The caller (the `revops` crate's guarded live
/// broadcaster constructor) MUST run this before the broadcaster it
/// constructs becomes usable, so a restart can never accept a fresh
/// cutover arm without first restoring whatever quarantine state the
/// prior process left behind.
///
/// Runs inside one `BEGIN IMMEDIATE` transaction (mirroring
/// [`commit_fee_cycle`]'s rollback-on-error shape): either every orphaned
/// row is marked ambiguous AND the quarantine entry (if needed) is
/// inserted, or neither happens -- a crash mid-reconciliation can never
/// leave some rows resolved without the corresponding quarantine in
/// place. Returns the number of attempts reconciled (`0` = nothing to
/// do).
pub fn reconcile_quarantine_on_restart(conn: &Connection, now: i64) -> Result<usize> {
    conn.execute_batch("BEGIN IMMEDIATE")
        .context("begin quarantine reconciliation transaction")?;
    let result = reconcile_quarantine_on_restart_locked(conn, now).and_then(|n| {
        // COMMIT inside the guarded result -- Task 42 correction F3's
        // defect class (a failed COMMIT must reach ROLLBACK too).
        conn.execute_batch("COMMIT")
            .context("commit quarantine reconciliation transaction")?;
        Ok(n)
    });
    if result.is_err() {
        let _ = conn.execute_batch("ROLLBACK");
    }
    result
}

fn reconcile_quarantine_on_restart_locked(conn: &Connection, now: i64) -> Result<usize> {
    let orphaned = unresolved_broadcast_attempts(conn)?;
    if orphaned.is_empty() {
        return Ok(0);
    }
    for row in &orphaned {
        record_broadcast_attempt_result(
            conn,
            row.id,
            BroadcastAttemptOutcome::Ambiguous,
            Some(
                "resolved via restart reconciliation: no result was recorded before the prior \
                 process exited",
            ),
            now,
        )?;
    }
    if active_quarantine(conn)?.is_none() {
        let last = orphaned
            .last()
            .expect("orphaned is non-empty (checked above)");
        insert_quarantine(
            conn,
            &QuarantineEntry {
                reason: format!(
                    "restart reconciliation: {} broadcast attempt(s) had no recorded result at \
                     startup",
                    orphaned.len()
                ),
                // Deliberately `None`, never `last.cycle_id` -- a live
                // broadcast attempt's `cycle_id` is NOT guaranteed to name
                // a row in `rust_fee_cycles` (that table belongs to the
                // shadow-commit pipeline; see this table's DDL comment),
                // and `rust_execution_quarantine.cycle_id` carries an FK
                // to it. Copying an attempt's own cycle_id straight
                // through would make reconciliation fail with a foreign
                // key violation for the common live-mode case.
                cycle_id: None,
                channel_id: Some(last.channel_id.clone()),
                request_id: Some(last.request_id.clone()),
                entered_at: now,
            },
        )?;
    }
    Ok(orphaned.len())
}

/// The most recently recorded runway snapshot, if any.
pub fn latest_runway_snapshot(conn: &Connection) -> Result<Option<RunwaySnapshotRow>> {
    conn.query_row(
        "SELECT snapshot_at, report_schema_version, source_commit, binary_sha256, summary_json
         FROM rust_runway_snapshots
         ORDER BY snapshot_at DESC, id DESC
         LIMIT 1",
        [],
        |r| {
            Ok(RunwaySnapshotRow {
                snapshot_at: r.get(0)?,
                report_schema_version: r.get(1)?,
                source_commit: r.get(2)?,
                binary_sha256: r.get(3)?,
                summary_json: r.get(4)?,
            })
        },
    )
    .optional()
    .context("read latest runway snapshot")
}

/// Insert one burned cutover-arm nonce. This table is a deny ledger: no read
/// path may use a row to grant authority.
///
/// Returns `Ok(true)` when the nonce was inserted, `Ok(false)` when the
/// primary key already holds it (the caller's `ReusedNonce` refusal --
/// distinguished typed, per Task 59 §5.3, from every other insert failure,
/// which is `Err` and maps to `ConsumeFailed`).
pub fn insert_consumed_nonce(
    conn: &Connection,
    nonce: &str,
    consumed_at: i64,
    source_commit: &str,
    binary_sha256: &str,
    arm_expires_at: i64,
) -> Result<bool> {
    let inserted = conn
        .execute(
            "INSERT OR IGNORE INTO rust_consumed_arm_nonces
             (nonce, consumed_at, source_commit, binary_sha256, arm_expires_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                nonce,
                consumed_at,
                source_commit,
                binary_sha256,
                arm_expires_at
            ],
        )
        .context("insert consumed cutover-arm nonce")?;
    Ok(inserted == 1)
}

#[derive(Clone, Copy)]
enum SweepTarget {
    Horizon {
        table: &'static str,
        key: &'static str,
    },
    Snapshots,
}

impl SweepTarget {
    const ORDERED: [Self; 4] = [
        Self::Horizon {
            table: "rust_fee_shadow_outcomes",
            key: "cycle_ts",
        },
        Self::Horizon {
            table: "rust_fee_trigger_events",
            key: "received_at",
        },
        Self::Horizon {
            table: "rust_mempool_ma_comparison",
            key: "at",
        },
        Self::Snapshots,
    ];

    fn table(self) -> &'static str {
        match self {
            Self::Horizon { table, .. } => table,
            Self::Snapshots => "rust_runway_snapshots",
        }
    }
}

/// Delete at most one batch from one Class-W table in its own immediate
/// transaction. SQL identifiers come exclusively from the private fixed enum.
fn sweep_one_batch(conn: &Connection, target: SweepTarget, horizon: i64) -> Result<usize> {
    let tx = conn
        .unchecked_transaction()
        .context("begin retention batch")?;
    let deleted = match target {
        SweepTarget::Horizon { table, key } => {
            let sql = format!(
                "DELETE FROM {table}
                 WHERE id IN (
                     SELECT id FROM {table}
                     WHERE {key} < ?1
                     ORDER BY {key} ASC, id ASC
                     LIMIT ?2
                 )"
            );
            tx.execute(&sql, params![horizon, RETENTION_BATCH_ROWS as i64])
                .with_context(|| format!("sweep {table}"))?
        }
        SweepTarget::Snapshots => tx
            .execute(
                "DELETE FROM rust_runway_snapshots
                 WHERE id IN (
                     SELECT id FROM rust_runway_snapshots
                     WHERE id NOT IN (
                         SELECT id FROM rust_runway_snapshots
                         ORDER BY id DESC
                         LIMIT ?1
                     )
                     ORDER BY id ASC
                     LIMIT ?2
                 )",
                params![SNAPSHOT_KEEP_LAST as i64, RETENTION_BATCH_ROWS as i64],
            )
            .context("sweep rust_runway_snapshots")?,
    };
    tx.commit().context("commit retention batch")?;
    Ok(deleted)
}

/// Run one globally bounded, fair Class-W retention sweep.
///
/// At most one batch is offered to a table before the cursor advances to
/// the next table, and at most [`RETENTION_MAX_BATCHES_PER_SWEEP`] nonempty
/// batches are committed across the whole sweep. A full empty round proves
/// that no eligible Class-W rows remain.
pub fn run_retention_sweep(
    conn: &Connection,
    now: i64,
    cursor: RetentionCursor,
) -> Result<RetentionReport> {
    let targets = SweepTarget::ORDERED;
    let mut report = RetentionReport::empty(cursor);
    let horizon = now.saturating_sub(RUNWAY_EVIDENCE_RETENTION_SECONDS);
    let mut index = cursor.index(targets.len());
    let mut empty_in_round = 0usize;

    while report.batches < RETENTION_MAX_BATCHES_PER_SWEEP && empty_in_round < targets.len() {
        let target = targets[index];
        let deleted = sweep_one_batch(conn, target, horizon)?;
        if deleted == 0 {
            empty_in_round += 1;
        } else {
            empty_in_round = 0;
            report.batches += 1;
            *report.deleted.entry(target.table()).or_insert(0) += deleted as u64;
            report.next_cursor = RetentionCursor::after(index, targets.len());
        }
        index = (index + 1) % targets.len();
    }
    report.truncated = report.batches == RETENTION_MAX_BATCHES_PER_SWEEP;
    Ok(report)
}

// ---------------------------------------------------------------------------
// Task 60: durable rebalance attempt/reservation rails (Class E)
// ---------------------------------------------------------------------------

/// One rebalance submission's pre-submit intent (Task 60 slice 1). The
/// insert also creates the ACTIVE reservation in the SAME transaction --
/// an attempt row without its reservation (or vice versa) must never be
/// observable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceAttemptIntent {
    pub request_id: String,
    pub source_channel: String,
    pub dest_channel: String,
    pub amount_sats: i64,
    pub max_fee_sats: i64,
    /// 'cycle' | 'manual' | 'manual_force' (CHECK-enforced).
    pub trigger: String,
    pub submitted_at: i64,
}

/// One attempt's TERMINAL settlement: outcome + reservation flip, applied
/// in one transaction, exactly once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceSettle {
    pub request_id: String,
    /// 'success' | 'rejected' | 'clean_failure_before_write' |
    /// 'outcome_unknown' (CHECK-enforced).
    pub outcome: String,
    pub outcome_detail: Option<String>,
    pub fee_paid_sats: Option<i64>,
    pub payment_hash: Option<String>,
    /// 'settled' | 'released' | 'quarantined' -- never back to 'active'.
    pub reservation_status: String,
    pub resolved_at: i64,
}

/// One `outcome IS NULL` attempt row, for restart reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnresolvedRebalanceAttempt {
    pub id: i64,
    pub request_id: String,
    pub source_channel: String,
    pub dest_channel: String,
    pub amount_sats: i64,
    pub submitted_at: i64,
    pub payment_hash: Option<String>,
}

/// Insert one rebalance intent row AND its `active` reservation
/// atomically. Returns the attempt row id. A duplicate `request_id`
/// surfaces the UNIQUE violation as a clean error (nothing written).
pub fn insert_rebalance_attempt(conn: &Connection, intent: &RebalanceAttemptIntent) -> Result<i64> {
    let tx = conn
        .unchecked_transaction()
        .context("begin rebalance intent txn")?;
    tx.execute(
        "INSERT INTO rust_rebalance_attempts
             (request_id, source_channel, dest_channel, amount_sats, max_fee_sats,
              trigger, submitted_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            intent.request_id,
            intent.source_channel,
            intent.dest_channel,
            intent.amount_sats,
            intent.max_fee_sats,
            intent.trigger,
            intent.submitted_at
        ],
    )
    .context("insert rebalance attempt intent (UNIQUE request_id)")?;
    let attempt_id = tx.last_insert_rowid();
    tx.execute(
        "INSERT INTO rust_rebalance_reservations
             (attempt_request_id, reserved_sats, reserved_at, status)
         VALUES (?1, ?2, ?3, 'active')",
        params![intent.request_id, intent.amount_sats, intent.submitted_at],
    )
    .context("insert active rebalance reservation")?;
    tx.commit().context("commit rebalance intent txn")?;
    Ok(attempt_id)
}

/// Apply one TERMINAL settlement atomically: the attempt's outcome and its
/// reservation's flip commit together, and only if the attempt is not
/// already terminal (`outcome IS NULL` guard -- the exactly-once rail).
pub fn settle_rebalance_attempt(conn: &Connection, settle: &RebalanceSettle) -> Result<()> {
    let tx = conn
        .unchecked_transaction()
        .context("begin rebalance settle txn")?;
    let updated = tx
        .execute(
            "UPDATE rust_rebalance_attempts
             SET outcome = ?2,
                 outcome_detail = ?3,
                 fee_paid_sats = ?4,
                 payment_hash = COALESCE(?5, payment_hash),
                 completed_at = ?6
             WHERE request_id = ?1 AND outcome IS NULL",
            params![
                settle.request_id,
                settle.outcome,
                settle.outcome_detail,
                settle.fee_paid_sats,
                settle.payment_hash,
                settle.resolved_at
            ],
        )
        .context("settle rebalance attempt outcome")?;
    if updated == 0 {
        anyhow::bail!(
            "rebalance attempt {} is already terminal (or unknown); refusing a second              terminal write",
            settle.request_id
        );
    }
    tx.execute(
        "UPDATE rust_rebalance_reservations
         SET status = ?2, settled_fee_sats = ?3, resolved_at = ?4
         WHERE attempt_request_id = ?1 AND status = 'active'",
        params![
            settle.request_id,
            settle.reservation_status,
            settle.fee_paid_sats,
            settle.resolved_at
        ],
    )
    .context("flip rebalance reservation")?;
    tx.commit().context("commit rebalance settle txn")?;
    Ok(())
}

/// Every attempt whose terminal outcome was never recorded -- the restart
/// reconciliation work list.
pub fn unresolved_rebalance_attempts(conn: &Connection) -> Result<Vec<UnresolvedRebalanceAttempt>> {
    let mut stmt = conn
        .prepare(
            "SELECT id, request_id, source_channel, dest_channel, amount_sats,
                    submitted_at, payment_hash
             FROM rust_rebalance_attempts
             WHERE outcome IS NULL
             ORDER BY id ASC",
        )
        .context("prepare unresolved rebalance attempts")?;
    let rows = stmt
        .query_map([], |row| {
            Ok(UnresolvedRebalanceAttempt {
                id: row.get(0)?,
                request_id: row.get(1)?,
                source_channel: row.get(2)?,
                dest_channel: row.get(3)?,
                amount_sats: row.get(4)?,
                submitted_at: row.get(5)?,
                payment_hash: row.get(6)?,
            })
        })
        .context("query unresolved rebalance attempts")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("read unresolved rebalance attempts")?;
    Ok(rows)
}

/// Sats currently held against the rebalance budget: ACTIVE reservations
/// plus QUARANTINED ones (an unknown outcome may still have spent the
/// money -- releasing it would over-spend the budget), at or after `since`.
pub fn active_rebalance_reserved_sats(conn: &Connection, since: i64) -> Result<i64> {
    conn.query_row(
        "SELECT COALESCE(SUM(reserved_sats), 0)
         FROM rust_rebalance_reservations
         WHERE status IN ('active', 'quarantined') AND reserved_at >= ?1",
        params![since],
        |row| row.get(0),
    )
    .context("sum active rebalance reservations")
}

// ---------------------------------------------------------------------------
// Task 62: durable capital intent/reservation rails (Class E)
// ---------------------------------------------------------------------------

/// One capital action's pre-submit intent (Task 62 slice 1): open/close/
/// defib. The insert also creates the ACTIVE reservation in the SAME
/// transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapitalIntent {
    pub request_id: String,
    /// 'open' | 'close' | 'defib' (CHECK-enforced).
    pub kind: String,
    pub peer_id: String,
    pub channel_id: Option<String>,
    pub amount_sats: i64,
    pub reason: Option<String>,
    pub submitted_at: i64,
}

/// One capital intent's TERMINAL settlement (exactly once).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapitalSettle {
    pub request_id: String,
    /// 'clean_refusal' | 'rejected' | 'success' | 'outcome_unknown'.
    pub outcome: String,
    pub outcome_detail: Option<String>,
    pub txid: Option<String>,
    /// 'settled' | 'released' | 'quarantined'.
    pub reservation_status: String,
    pub settled_sats: Option<i64>,
    pub resolved_at: i64,
}

/// One `outcome IS NULL` capital intent, for restart reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnresolvedCapitalIntent {
    pub id: i64,
    pub request_id: String,
    pub kind: String,
    pub peer_id: String,
    pub channel_id: Option<String>,
    pub amount_sats: i64,
    pub submitted_at: i64,
}

/// Insert one capital intent AND its `active` reservation atomically.
pub fn insert_capital_intent(conn: &Connection, intent: &CapitalIntent) -> Result<i64> {
    let tx = conn
        .unchecked_transaction()
        .context("begin capital intent txn")?;
    tx.execute(
        "INSERT INTO rust_capital_intents
             (request_id, kind, peer_id, channel_id, amount_sats, reason, submitted_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            intent.request_id,
            intent.kind,
            intent.peer_id,
            intent.channel_id,
            intent.amount_sats,
            intent.reason,
            intent.submitted_at
        ],
    )
    .context("insert capital intent (UNIQUE request_id)")?;
    let intent_id = tx.last_insert_rowid();
    tx.execute(
        "INSERT INTO rust_capital_reservations
             (attempt_request_id, reserved_sats, reserved_at, status)
         VALUES (?1, ?2, ?3, 'active')",
        params![intent.request_id, intent.amount_sats, intent.submitted_at],
    )
    .context("insert active capital reservation")?;
    tx.commit().context("commit capital intent txn")?;
    Ok(intent_id)
}

/// Apply one TERMINAL capital settlement atomically, exactly once.
pub fn settle_capital_intent(conn: &Connection, settle: &CapitalSettle) -> Result<()> {
    let tx = conn
        .unchecked_transaction()
        .context("begin capital settle txn")?;
    let updated = tx
        .execute(
            "UPDATE rust_capital_intents
             SET outcome = ?2, outcome_detail = ?3, txid = COALESCE(?4, txid),
                 completed_at = ?5
             WHERE request_id = ?1 AND outcome IS NULL",
            params![
                settle.request_id,
                settle.outcome,
                settle.outcome_detail,
                settle.txid,
                settle.resolved_at
            ],
        )
        .context("settle capital intent outcome")?;
    if updated == 0 {
        anyhow::bail!(
            "capital intent {} is already terminal (or unknown); refusing a second \
             terminal write",
            settle.request_id
        );
    }
    tx.execute(
        "UPDATE rust_capital_reservations
         SET status = ?2, settled_sats = ?3, resolved_at = ?4
         WHERE attempt_request_id = ?1 AND status = 'active'",
        params![
            settle.request_id,
            settle.reservation_status,
            settle.settled_sats,
            settle.resolved_at
        ],
    )
    .context("flip capital reservation")?;
    tx.commit().context("commit capital settle txn")?;
    Ok(())
}

/// Every capital intent whose terminal outcome was never recorded.
pub fn unresolved_capital_intents(conn: &Connection) -> Result<Vec<UnresolvedCapitalIntent>> {
    let mut stmt = conn
        .prepare(
            "SELECT id, request_id, kind, peer_id, channel_id, amount_sats, submitted_at
             FROM rust_capital_intents
             WHERE outcome IS NULL
             ORDER BY id ASC",
        )
        .context("prepare unresolved capital intents")?;
    let rows = stmt
        .query_map([], |row| {
            Ok(UnresolvedCapitalIntent {
                id: row.get(0)?,
                request_id: row.get(1)?,
                kind: row.get(2)?,
                peer_id: row.get(3)?,
                channel_id: row.get(4)?,
                amount_sats: row.get(5)?,
                submitted_at: row.get(6)?,
            })
        })
        .context("query unresolved capital intents")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("read unresolved capital intents")?;
    Ok(rows)
}

/// Sats held against the capital budget: ACTIVE + QUARANTINED
/// reservations at or after `since` (an unknown outcome may have spent
/// on-chain funds -- releasing it would over-spend).
pub fn active_capital_reserved_sats(conn: &Connection, since: i64) -> Result<i64> {
    conn.query_row(
        "SELECT COALESCE(SUM(reserved_sats), 0)
         FROM rust_capital_reservations
         WHERE status IN ('active', 'quarantined') AND reserved_at >= ?1",
        params![since],
        |row| row.get(0),
    )
    .context("sum active capital reservations")
}
