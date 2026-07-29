//! Task 65 slice 1: the writable production-schema actor -- the WRITE
//! sibling of `actor::spawn_read_only`, for the canonical live
//! state-writer rail.
//!
//! Contracts:
//!
//! - **Opens EXISTING files only** (`SQLITE_OPEN_READWRITE`, no CREATE):
//!   the production database is Python-owned; a missing file is a typed
//!   refusal, never a fresh database.
//! - **Schema identity check before any command**: the four write-target
//!   tables must exist with their required columns (PRAGMA table_info),
//!   else a typed `SchemaMismatch` naming the table and detail. The
//!   writer CREATEs and ALTERs nothing, ever.
//! - **Single owner**: one `Connection` on one blocking task; only
//!   command/reply messages cross the boundary (the `owner.rs`/`actor.rs`
//!   discipline).
//! - **Python-parity write semantics**, verbatim from
//!   `cl_revenue_ops/modules/database.py`:
//!   - config versions are computed INSIDE `BEGIN IMMEDIATE` and the
//!     in-transaction value is returned (M-13 v2: a post-commit re-read
//!     could misattribute a concurrent writer's version, and INSERT OR
//!     REPLACE of the max-version row would regress a post-hoc MAX).
//!   - budget transitions guard on `status = 'active'` -- terminal rows
//!     never resurrect; zero-row updates are classified
//!     `AlreadyTerminal` vs `NotFound` inside the same transaction.
//!   - the closed-channel purge deletes from the exact
//!     `remove_closed_channel_data` table set (database.py:6592-6665),
//!     including both directions of `pair_rebalance_failures`.
//! - **Batches are bounded to 100 and transactional**: an over-bound
//!   batch is refused WHOLE (typed), and any mid-batch failure rolls the
//!   entire batch back.

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OpenFlags};
use std::path::Path;
use tokio::sync::{mpsc, oneshot};

/// The hard batch bound (the Task 65 contract's number).
pub const STATE_WRITER_BATCH_BOUND: usize = 100;

/// Typed open-time refusals -- fail-closed identity checks.
#[derive(Debug)]
pub enum StateWriterOpenError {
    /// The database file does not exist; the writer never creates one.
    MissingFile(String),
    /// A required table/column is absent or misshapen.
    SchemaMismatch { table: String, detail: String },
    /// Anything else (io error, lock trouble at open).
    OpenFailed(String),
}

impl std::fmt::Display for StateWriterOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingFile(path) => {
                write!(
                    f,
                    "state_writer_missing_file: {path} (the writer never creates it)"
                )
            }
            Self::SchemaMismatch { table, detail } => {
                write!(f, "state_writer_schema_mismatch: table {table}: {detail}")
            }
            Self::OpenFailed(detail) => write!(f, "state_writer_open_failed: {detail}"),
        }
    }
}

impl std::error::Error for StateWriterOpenError {}

/// One peer-policy upsert row (py `peer_policies`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerPolicyWrite {
    pub peer_id: String,
    pub strategy: String,
    pub rebalance_mode: String,
    pub fee_ppm_target: Option<i64>,
    pub tags: Option<String>,
}

/// Config delete outcome (durable either way).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigDelete {
    Deleted,
    AlreadyAbsent,
}

/// Guarded budget transition outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetTransition {
    Applied,
    /// The row exists but is no longer `active` -- terminal states never
    /// resurrect (py guard `AND status = 'active'`).
    AlreadyTerminal,
    NotFound,
}

/// Bounded-batch outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchAck {
    Applied {
        count: usize,
    },
    /// Refused WHOLE: nothing written.
    DeniedOverBound {
        len: usize,
    },
}

enum Command {
    SetConfigOverride {
        key: String,
        value: String,
        reply: oneshot::Sender<Result<i64>>,
    },
    DeleteConfigOverride {
        key: String,
        reply: oneshot::Sender<Result<ConfigDelete>>,
    },
    UpsertPeerPolicy {
        write: PeerPolicyWrite,
        now: i64,
        reply: oneshot::Sender<Result<()>>,
    },
    ApplyPolicyBatch {
        writes: Vec<PeerPolicyWrite>,
        now: i64,
        reply: oneshot::Sender<Result<BatchAck>>,
    },
    SetHotChannelOverride {
        peer_id: String,
        note: Option<String>,
        min_depletion_trigger_pct: Option<f64>,
        now: i64,
        reply: oneshot::Sender<Result<()>>,
    },
    RemoveHotChannelOverride {
        peer_id: String,
        reply: oneshot::Sender<Result<bool>>,
    },
    ReleaseBudgetReservation {
        reservation_id: String,
        reply: oneshot::Sender<Result<BudgetTransition>>,
    },
    MarkBudgetSpent {
        reservation_id: String,
        actual_spent: i64,
        reply: oneshot::Sender<Result<BudgetTransition>>,
    },
    CleanupStaleReservations {
        max_age_seconds: i64,
        now: i64,
        reply: oneshot::Sender<Result<i64>>,
    },
    CleanupClosedChannels {
        channel_ids: Vec<String>,
        reply: oneshot::Sender<Result<BatchAck>>,
    },
}

/// Cheap cloneable handle to the writer actor.
#[derive(Clone, Debug)]
pub struct StateWriterHandle {
    tx: mpsc::Sender<Command>,
}

macro_rules! roundtrip {
    ($self:ident, $variant:expr) => {{
        let (reply, rx) = oneshot::channel();
        $self
            .tx
            .send($variant(reply))
            .await
            .context("state writer actor gone")?;
        rx.await.context("state writer actor dropped reply")?
    }};
}

impl StateWriterHandle {
    pub async fn set_config_override(&self, key: String, value: String) -> Result<i64> {
        roundtrip!(self, |reply| Command::SetConfigOverride {
            key,
            value,
            reply
        })
    }

    pub async fn delete_config_override(&self, key: String) -> Result<ConfigDelete> {
        roundtrip!(self, |reply| Command::DeleteConfigOverride { key, reply })
    }

    pub async fn upsert_peer_policy(&self, write: PeerPolicyWrite, now: i64) -> Result<()> {
        roundtrip!(self, |reply| Command::UpsertPeerPolicy {
            write,
            now,
            reply
        })
    }

    pub async fn apply_policy_batch(
        &self,
        writes: Vec<PeerPolicyWrite>,
        now: i64,
    ) -> Result<BatchAck> {
        roundtrip!(self, |reply| Command::ApplyPolicyBatch {
            writes,
            now,
            reply
        })
    }

    pub async fn set_hot_channel_override(
        &self,
        peer_id: String,
        note: Option<String>,
        min_depletion_trigger_pct: Option<f64>,
        now: i64,
    ) -> Result<()> {
        roundtrip!(self, |reply| Command::SetHotChannelOverride {
            peer_id,
            note,
            min_depletion_trigger_pct,
            now,
            reply
        })
    }

    pub async fn remove_hot_channel_override(&self, peer_id: String) -> Result<bool> {
        roundtrip!(self, |reply| Command::RemoveHotChannelOverride {
            peer_id,
            reply
        })
    }

    pub async fn release_budget_reservation(
        &self,
        reservation_id: String,
    ) -> Result<BudgetTransition> {
        roundtrip!(self, |reply| Command::ReleaseBudgetReservation {
            reservation_id,
            reply
        })
    }

    pub async fn mark_budget_spent(
        &self,
        reservation_id: String,
        actual_spent: i64,
    ) -> Result<BudgetTransition> {
        roundtrip!(self, |reply| Command::MarkBudgetSpent {
            reservation_id,
            actual_spent,
            reply
        })
    }

    pub async fn cleanup_stale_reservations(&self, max_age_seconds: i64, now: i64) -> Result<i64> {
        roundtrip!(self, |reply| Command::CleanupStaleReservations {
            max_age_seconds,
            now,
            reply
        })
    }

    pub async fn cleanup_closed_channels(&self, channel_ids: Vec<String>) -> Result<BatchAck> {
        roundtrip!(self, |reply| Command::CleanupClosedChannels {
            channel_ids,
            reply
        })
    }
}

/// The write-target tables and their REQUIRED columns (schema identity
/// check). Python owns the DDL; the writer only verifies it.
const REQUIRED_SCHEMA: &[(&str, &[&str])] = &[
    (
        "config_overrides",
        &["key", "value", "version", "updated_at"],
    ),
    (
        "peer_policies",
        &[
            "peer_id",
            "strategy",
            "rebalance_mode",
            "fee_ppm_target",
            "tags",
            "updated_at",
        ],
    ),
    (
        "hot_channel_protection_overrides",
        &["peer_id", "added_at", "note", "min_depletion_trigger_pct"],
    ),
    (
        "budget_reservations",
        &[
            "reservation_id",
            "reserved_sats",
            "reserved_at",
            "job_channel_id",
            "status",
        ],
    ),
];

fn verify_schema(conn: &Connection) -> std::result::Result<(), StateWriterOpenError> {
    for (table, columns) in REQUIRED_SCHEMA {
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .map_err(|e| StateWriterOpenError::OpenFailed(e.to_string()))?;
        let present: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(|e| StateWriterOpenError::OpenFailed(e.to_string()))?
            .collect::<rusqlite::Result<_>>()
            .map_err(|e| StateWriterOpenError::OpenFailed(e.to_string()))?;
        if present.is_empty() {
            return Err(StateWriterOpenError::SchemaMismatch {
                table: table.to_string(),
                detail: "table missing".to_string(),
            });
        }
        for column in *columns {
            if !present.iter().any(|c| c == column) {
                return Err(StateWriterOpenError::SchemaMismatch {
                    table: table.to_string(),
                    detail: format!("required column {column} missing"),
                });
            }
        }
    }
    Ok(())
}

/// Spawn the writer actor over an EXISTING production-schema database.
pub async fn spawn_state_writer(
    path: &Path,
) -> std::result::Result<StateWriterHandle, StateWriterOpenError> {
    if !path.exists() {
        return Err(StateWriterOpenError::MissingFile(
            path.display().to_string(),
        ));
    }
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| StateWriterOpenError::OpenFailed(e.to_string()))?;
    conn.busy_timeout(std::time::Duration::from_millis(crate::BUSY_TIMEOUT_MS))
        .map_err(|e| StateWriterOpenError::OpenFailed(e.to_string()))?;
    verify_schema(&conn)?;

    let (tx, mut rx) = mpsc::channel::<Command>(64);
    tokio::task::spawn_blocking(move || {
        while let Some(cmd) = rx.blocking_recv() {
            match cmd {
                Command::SetConfigOverride { key, value, reply } => {
                    let _ = reply.send(set_config_override(&conn, &key, &value));
                }
                Command::DeleteConfigOverride { key, reply } => {
                    let _ = reply.send(delete_config_override(&conn, &key));
                }
                Command::UpsertPeerPolicy { write, now, reply } => {
                    let _ = reply.send(upsert_peer_policy(&conn, &write, now));
                }
                Command::ApplyPolicyBatch { writes, now, reply } => {
                    let _ = reply.send(apply_policy_batch(&conn, &writes, now));
                }
                Command::SetHotChannelOverride {
                    peer_id,
                    note,
                    min_depletion_trigger_pct,
                    now,
                    reply,
                } => {
                    let _ = reply.send(set_hot_channel_override(
                        &conn,
                        &peer_id,
                        note.as_deref(),
                        min_depletion_trigger_pct,
                        now,
                    ));
                }
                Command::RemoveHotChannelOverride { peer_id, reply } => {
                    let _ = reply.send(remove_hot_channel_override(&conn, &peer_id));
                }
                Command::ReleaseBudgetReservation {
                    reservation_id,
                    reply,
                } => {
                    let _ = reply.send(budget_transition(&conn, &reservation_id, "released", None));
                }
                Command::MarkBudgetSpent {
                    reservation_id,
                    actual_spent,
                    reply,
                } => {
                    let _ = reply.send(budget_transition(
                        &conn,
                        &reservation_id,
                        "spent",
                        Some(actual_spent),
                    ));
                }
                Command::CleanupStaleReservations {
                    max_age_seconds,
                    now,
                    reply,
                } => {
                    let _ = reply.send(cleanup_stale_reservations(&conn, max_age_seconds, now));
                }
                Command::CleanupClosedChannels { channel_ids, reply } => {
                    let _ = reply.send(cleanup_closed_channels(&conn, &channel_ids));
                }
            }
        }
    });
    Ok(StateWriterHandle { tx })
}

/// Run `work` inside an explicit `BEGIN IMMEDIATE` transaction (the py
/// parity primitive -- rusqlite's typed immediate-transaction helper is
/// unavailable on `&Connection` in this version).
fn immediate_txn<T>(
    conn: &Connection,
    what: &str,
    work: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    conn.execute_batch("BEGIN IMMEDIATE")
        .with_context(|| format!("begin {what} txn"))?;
    match work(conn) {
        Ok(value) => {
            conn.execute_batch("COMMIT")
                .with_context(|| format!("commit {what}"))?;
            Ok(value)
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// py `set_config_override` (database.py:7324-7362), verbatim semantics:
/// version = MAX+1 computed INSIDE `BEGIN IMMEDIATE`, returned from the
/// transaction.
fn set_config_override(conn: &Connection, key: &str, value: &str) -> Result<i64> {
    immediate_txn(conn, "config override", |tx| {
        let current_max: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM config_overrides",
                [],
                |r| r.get(0),
            )
            .context("read current max config version")?;
        let new_version = current_max + 1;
        tx.execute(
            "INSERT OR REPLACE INTO config_overrides (key, value, version, updated_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![key, value, new_version, now_unix()],
        )
        .context("write config override")?;
        Ok(new_version)
    })
}

fn delete_config_override(conn: &Connection, key: &str) -> Result<ConfigDelete> {
    let deleted = conn
        .execute("DELETE FROM config_overrides WHERE key = ?1", params![key])
        .context("delete config override")?;
    Ok(if deleted > 0 {
        ConfigDelete::Deleted
    } else {
        ConfigDelete::AlreadyAbsent
    })
}

fn upsert_peer_policy(conn: &Connection, write: &PeerPolicyWrite, now: i64) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO peer_policies
             (peer_id, strategy, rebalance_mode, fee_ppm_target, tags, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            write.peer_id,
            write.strategy,
            write.rebalance_mode,
            write.fee_ppm_target,
            write.tags,
            now
        ],
    )
    .context("upsert peer policy")?;
    Ok(())
}

fn apply_policy_batch(conn: &Connection, writes: &[PeerPolicyWrite], now: i64) -> Result<BatchAck> {
    if writes.len() > STATE_WRITER_BATCH_BOUND {
        return Ok(BatchAck::DeniedOverBound { len: writes.len() });
    }
    immediate_txn(conn, "policy batch", |tx| {
        for write in writes {
            tx.execute(
                "INSERT OR REPLACE INTO peer_policies
                     (peer_id, strategy, rebalance_mode, fee_ppm_target, tags, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    write.peer_id,
                    write.strategy,
                    write.rebalance_mode,
                    write.fee_ppm_target,
                    write.tags,
                    now
                ],
            )
            .with_context(|| format!("policy batch write for {}", write.peer_id))?;
        }
        Ok(BatchAck::Applied {
            count: writes.len(),
        })
    })
}

fn set_hot_channel_override(
    conn: &Connection,
    peer_id: &str,
    note: Option<&str>,
    min_depletion_trigger_pct: Option<f64>,
    now: i64,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO hot_channel_protection_overrides
             (peer_id, added_at, note, min_depletion_trigger_pct)
         VALUES (?1, ?2, ?3, ?4)",
        params![peer_id, now, note, min_depletion_trigger_pct],
    )
    .context("set hot channel override")?;
    Ok(())
}

fn remove_hot_channel_override(conn: &Connection, peer_id: &str) -> Result<bool> {
    let deleted = conn
        .execute(
            "DELETE FROM hot_channel_protection_overrides WHERE peer_id = ?1",
            params![peer_id],
        )
        .context("remove hot channel override")?;
    Ok(deleted > 0)
}

/// py release/mark-spent (database.py:3748/:3772): the UPDATE guards
/// `status = 'active'`; a zero-row update is disambiguated inside the
/// SAME transaction (AlreadyTerminal vs NotFound).
fn budget_transition(
    conn: &Connection,
    reservation_id: &str,
    to_status: &str,
    _actual_spent: Option<i64>,
) -> Result<BudgetTransition> {
    immediate_txn(conn, "budget transition", |tx| {
        let updated = tx
            .execute(
                "UPDATE budget_reservations SET status = ?2
                 WHERE reservation_id = ?1 AND status = 'active'",
                params![reservation_id, to_status],
            )
            .context("guarded budget transition")?;
        if updated > 0 {
            return Ok(BudgetTransition::Applied);
        }
        let exists: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM budget_reservations WHERE reservation_id = ?1",
                params![reservation_id],
                |r| r.get(0),
            )
            .context("disambiguate zero-row transition")?;
        Ok(if exists > 0 {
            BudgetTransition::AlreadyTerminal
        } else {
            BudgetTransition::NotFound
        })
    })
}

/// py `cleanup_stale_reservations` (database.py:3802): release actives
/// older than the cutoff. (The Python pending_settlement carve-out lives
/// in `budget_reservations`-adjacent rebalance rows, not this table's
/// statuses -- actives are the only transition source here, same as py.)
fn cleanup_stale_reservations(conn: &Connection, max_age_seconds: i64, now: i64) -> Result<i64> {
    let cutoff = now - max_age_seconds;
    let released = conn
        .execute(
            "UPDATE budget_reservations SET status = 'released'
             WHERE status = 'active' AND reserved_at < ?1",
            params![cutoff],
        )
        .context("cleanup stale reservations")?;
    Ok(released as i64)
}

/// py `remove_closed_channel_data` (database.py:6592-6665), per channel,
/// all channels in ONE transaction (bounded batch): channel_states,
/// channel_failures, channel_probes, kalman_state, both directions of
/// pair_rebalance_failures, fee_strategy_state. The two py
/// tolerate-missing-table arms (pair_rebalance_failures,
/// fee_strategy_state) are mirrored.
fn cleanup_closed_channels(conn: &Connection, channel_ids: &[String]) -> Result<BatchAck> {
    if channel_ids.len() > STATE_WRITER_BATCH_BOUND {
        return Ok(BatchAck::DeniedOverBound {
            len: channel_ids.len(),
        });
    }
    immediate_txn(conn, "closed-channel purge", |tx| {
        for channel_id in channel_ids {
            for sql in [
                "DELETE FROM channel_states WHERE channel_id = ?1",
                "DELETE FROM channel_failures WHERE channel_id = ?1",
                "DELETE FROM channel_probes WHERE channel_id = ?1",
                "DELETE FROM kalman_state WHERE channel_id = ?1",
            ] {
                tx.execute(sql, params![channel_id])
                    .with_context(|| format!("purge {channel_id}"))?;
            }
            for tolerated in [
                "DELETE FROM pair_rebalance_failures
                 WHERE source_channel_id = ?1 OR dest_channel_id = ?1",
                "DELETE FROM fee_strategy_state WHERE channel_id = ?1",
            ] {
                match tx.execute(tolerated, params![channel_id]) {
                    Ok(_) => {}
                    // py: `except sqlite3.OperationalError: pass` --
                    // older schemas may lack these tables.
                    Err(e) if e.to_string().contains("no such table") => {}
                    Err(e) => {
                        return Err(e).with_context(|| format!("purge {channel_id}"));
                    }
                }
            }
        }
        Ok(BatchAck::Applied {
            count: channel_ids.len(),
        })
    })
}
