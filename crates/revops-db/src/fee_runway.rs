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

/// Idempotent `CREATE TABLE IF NOT EXISTS` (+ enabling foreign key
/// enforcement, a per-connection SQLite setting) for every Task 4 table.
/// Safe to call on every connection open, including a connection that
/// already has these tables.
pub fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(DDL).context("init fee runway schema")?;
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
pub fn record_seed_event(conn: &Connection, ev: &FeeSeedEventRow) -> Result<i64> {
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
    .context("insert fee seed event row")?;
    Ok(conn.last_insert_rowid())
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
