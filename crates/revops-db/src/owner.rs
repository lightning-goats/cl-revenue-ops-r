//! Single-owner read-write actor for the Rust plugin's OWN
//! notification-ingestion database (never the production DB -- see the
//! phase1b plan's Global Constraints). Mirrors `actor.rs`'s
//! single-owner-task pattern (the `rusqlite::Connection` never crosses a
//! task boundary, only request/reply messages do) but for a writable
//! connection that this plugin creates itself if it doesn't exist yet,
//! rather than `actor::spawn_read_only`'s "never creates the file"
//! contract over the production db.

use crate::notifications::{self, ForwardRow};
use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;
use tokio::sync::{mpsc, oneshot};

enum Command {
    InsertForward {
        row: ForwardRow,
        reply: oneshot::Sender<Result<bool>>,
    },
    LastForwardTs(oneshot::Sender<Result<Option<i64>>>),
    InsertPeerConnectionEvent {
        peer_id: String,
        event_type: String,
        ts: i64,
        reply: oneshot::Sender<Result<()>>,
    },
    InsertChannelClosureEvent {
        scid: String,
        cause: String,
        ts: i64,
        reply: oneshot::Sender<Result<()>>,
    },
}

/// Cheap, `Clone`-able handle to the observer-db owner task.
#[derive(Clone, Debug)]
pub struct ObserverHandle {
    tx: mpsc::Sender<Command>,
}

impl ObserverHandle {
    /// `INSERT OR IGNORE` a settled forward. Returns whether a new row was
    /// actually inserted (`false` on an exact-duplicate dedup no-op).
    pub async fn insert_forward(&self, row: ForwardRow) -> Result<bool> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::InsertForward { row, reply })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// `SELECT MAX(timestamp) FROM ingested_forwards`.
    pub async fn last_forward_ts(&self) -> Result<Option<i64>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::LastForwardTs(reply))
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Record a peer connect/disconnect event.
    pub async fn insert_peer_connection_event(
        &self,
        peer_id: String,
        event_type: String,
        ts: i64,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::InsertPeerConnectionEvent {
                peer_id,
                event_type,
                ts,
                reply,
            })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Record a channel-closure event.
    pub async fn insert_channel_closure_event(
        &self,
        scid: String,
        cause: String,
        ts: i64,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::InsertChannelClosureEvent {
                scid,
                cause,
                ts,
                reply,
            })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }
}

/// Open (creating the file and any missing parent directories if needed)
/// the plugin's own read-write sqlite file, initialize its schema, and
/// spawn the single-owner actor task that owns the resulting `Connection`
/// for the rest of the plugin's lifetime.
///
/// Unlike `actor::spawn_read_only` (which never creates the production
/// db), this is a fresh, Rust-only file with no production analog -- an
/// operator pointing `observer-db-path` at a path that doesn't exist yet
/// is the expected first-run case, not a misconfiguration.
pub async fn spawn_read_write(path: &Path) -> Result<ObserverHandle> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create observer db directory {}", parent.display()))?;
        }
    }
    let conn =
        Connection::open(path).with_context(|| format!("open observer db {}", path.display()))?;
    notifications::init_schema(&conn).context("init observer db schema")?;

    let (tx, mut rx) = mpsc::channel::<Command>(64);
    tokio::task::spawn_blocking(move || {
        // Single-owner: this `Connection` (which is `!Sync`) never leaves
        // this blocking task; only command/reply messages cross the
        // boundary. WAL + a single writer task means no cross-task lock
        // contention on our own db file.
        while let Some(cmd) = rx.blocking_recv() {
            match cmd {
                Command::InsertForward { row, reply } => {
                    let result = notifications::insert_forward_ignore_dup(&conn, &row);
                    let _ = reply.send(result);
                }
                Command::LastForwardTs(reply) => {
                    let result = notifications::last_forward_ts(&conn);
                    let _ = reply.send(result);
                }
                Command::InsertPeerConnectionEvent {
                    peer_id,
                    event_type,
                    ts,
                    reply,
                } => {
                    let result = notifications::insert_peer_connection_event(
                        &conn,
                        &peer_id,
                        &event_type,
                        ts,
                    );
                    let _ = reply.send(result);
                }
                Command::InsertChannelClosureEvent {
                    scid,
                    cause,
                    ts,
                    reply,
                } => {
                    let result =
                        notifications::insert_channel_closure_event(&conn, &scid, &cause, ts);
                    let _ = reply.send(result);
                }
            }
        }
    });
    Ok(ObserverHandle { tx })
}
