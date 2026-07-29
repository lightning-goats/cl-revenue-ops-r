//! Explicit retention classification for the Rust-owned observer database.
//!
//! Only the periodic-sweep subset of [`WINDOWED_TABLES`] may appear in
//! automated sweep statements. Every other application table is deliberately
//! retained: append-only evidence/identity, bounded current state, or deferred
//! notification data whose safe resume-cursor design belongs to a later task.

use std::collections::BTreeMap;

pub const RUNWAY_EVIDENCE_RETENTION_SECONDS: i64 = 30 * 86_400;
pub const SNAPSHOT_KEEP_LAST: u64 = 90;
pub const RETENTION_BATCH_ROWS: usize = 500;
pub const RETENTION_MAX_BATCHES_PER_SWEEP: usize = 8;

/// Class E: append-only audit and replay identity. Never swept.
pub const EXCLUDED_TABLES: &[&str] = &[
    "rust_fee_cycles",
    "rust_fee_requests",
    "rust_fee_ledger",
    "rust_broadcast_attempts",
    "rust_execution_quarantine",
    "rust_fee_seed_events",
    "rust_fee_restart_markers",
    "rust_consumed_arm_nonces",
];

/// Class C: current-state rows, bounded by key/upsert construction.
pub const CURRENT_STATE_TABLES: &[&str] = &[
    "rust_fee_state_generation",
    "rust_fee_state",
    "rust_loop_health",
];

/// Class D: explicitly classified but not pruned until a durable resume
/// cursor exists independently of the rows themselves.
pub const DEFERRED_TABLES: &[&str] = &[
    "ingested_forwards",
    "peer_connection_events",
    "channel_closure_events",
];

/// Class W: windowed evidence. The periodic sweep targets the first four
/// entries. `rust_mempool_fee_history` is bounded atomically at insert time
/// and therefore is deliberately not double-managed by the periodic sweep.
pub const WINDOWED_TABLES: &[&str] = &[
    "rust_fee_shadow_outcomes",
    "rust_fee_trigger_events",
    "rust_mempool_ma_comparison",
    "rust_runway_snapshots",
    "rust_mempool_fee_history",
];

/// SQLite-owned tables created by this schema. They are explicitly named so
/// the classification lint never silently filters an internal table.
pub const SQLITE_INTERNAL: &[&str] = &["sqlite_sequence"];

/// Cross-sweep fairness cursor. The value is always normalized before use.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionCursor(pub(crate) usize);

impl RetentionCursor {
    pub(crate) fn index(self, table_count: usize) -> usize {
        self.0 % table_count.max(1)
    }

    pub(crate) fn after(index: usize, table_count: usize) -> Self {
        Self((index + 1) % table_count.max(1))
    }
}

/// One bounded sweep's observable result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionReport {
    pub deleted: BTreeMap<&'static str, u64>,
    pub truncated: bool,
    pub batches: usize,
    pub next_cursor: RetentionCursor,
}

impl RetentionReport {
    pub(crate) fn empty(cursor: RetentionCursor) -> Self {
        Self {
            deleted: BTreeMap::new(),
            truncated: false,
            batches: 0,
            next_cursor: cursor,
        }
    }
}
