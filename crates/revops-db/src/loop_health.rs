use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};

pub const MAX_ERROR_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LoopId {
    /// Task 67: the three previously-missing analytics/startup loops.
    FlowAnalysis,
    Fee,
    Rebalance,
    StartupSnapshot,
    FinancialSnapshot,
    Boltz,
    Planner,
    LnPlus,
}

/// The five retained business/startup loops. Retired loop identities remain
/// parseable solely so historical health rows stay readable.
pub const REQUIRED_LOOPS: [LoopId; 5] = [
    LoopId::FlowAnalysis,
    LoopId::Fee,
    LoopId::Rebalance,
    LoopId::StartupSnapshot,
    LoopId::FinancialSnapshot,
];

impl LoopId {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FlowAnalysis => "flow-analysis",
            Self::Fee => "fee-adjustment",
            Self::Rebalance => "rebalance-check",
            Self::StartupSnapshot => "startup-snapshot",
            Self::FinancialSnapshot => "financial-snapshot",
            Self::Boltz => "boltz-auto-cycle",
            Self::Planner => "capacity-planner",
            Self::LnPlus => "lnplus-watcher",
        }
    }
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "flow-analysis" => Ok(Self::FlowAnalysis),
            "fee-adjustment" => Ok(Self::Fee),
            "rebalance-check" => Ok(Self::Rebalance),
            "startup-snapshot" => Ok(Self::StartupSnapshot),
            "financial-snapshot" => Ok(Self::FinancialSnapshot),
            "boltz-auto-cycle" => Ok(Self::Boltz),
            "capacity-planner" => Ok(Self::Planner),
            "lnplus-watcher" => Ok(Self::LnPlus),
            other => bail!("unknown loop identity {other:?}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WiringStatus {
    NotWired,
    Ready,
}

impl WiringStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotWired => "not_wired",
            Self::Ready => "ready",
        }
    }
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "not_wired" => Ok(Self::NotWired),
            "ready" => Ok(Self::Ready),
            other => bail!("unknown loop wiring status {other:?}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalStatus {
    None,
    Passed,
    Error,
}
impl TerminalStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Passed => "passed",
            Self::Error => "error",
        }
    }
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "none" => Ok(Self::None),
            "passed" => Ok(Self::Passed),
            "error" => Ok(Self::Error),
            other => bail!("unknown terminal status {other:?}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeStatus {
    Active,
    Suspended,
}
impl RuntimeStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Suspended => "suspended",
        }
    }
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "active" => Ok(Self::Active),
            "suspended" => Ok(Self::Suspended),
            other => bail!("unknown loop runtime status {other:?}"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoopHealthRow {
    pub loop_id: LoopId,
    pub wiring_status: WiringStatus,
    pub generation: u64,
    pub terminal_generation: u64,
    pub terminal_status: TerminalStatus,
    pub runtime_status: RuntimeStatus,
    pub last_suspended_at: Option<i64>,
    pub last_suspension_reason: Option<String>,
    pub last_started_at: Option<i64>,
    pub last_passed_at: Option<i64>,
    pub last_error_at: Option<i64>,
    pub last_error: Option<String>,
    pub coalesced_total: u64,
    pub dropped_total: u64,
    pub updated_at: i64,
    /// The process that most recently BEGAN a pass.
    pub boot_id: Option<String>,
    /// The process that recorded the terminal.
    pub terminal_boot_id: Option<String>,
}

impl LoopHealthRow {
    pub fn new(loop_id: LoopId, wiring_status: WiringStatus, updated_at: i64) -> Self {
        Self {
            loop_id,
            wiring_status,
            generation: 0,
            terminal_generation: 0,
            terminal_status: TerminalStatus::None,
            runtime_status: RuntimeStatus::Active,
            last_suspended_at: None,
            last_suspension_reason: None,
            last_started_at: None,
            last_passed_at: None,
            last_error_at: None,
            last_error: None,
            coalesced_total: 0,
            dropped_total: 0,
            updated_at,
            boot_id: None,
            terminal_boot_id: None,
        }
    }
}

const CANONICAL_COLUMNS: [&str; 17] = [
    "loop_name",
    "wiring_status",
    "generation",
    "terminal_generation",
    "terminal_status",
    "runtime_status",
    "last_suspended_at",
    "last_suspension_reason",
    "last_started_at",
    "last_passed_at",
    "last_error_at",
    "last_error",
    "coalesced_total",
    "dropped_total",
    "updated_at",
    // Task 67: boot binding. `boot_id` is the process that most recently
    // BEGAN a pass; `terminal_boot_id` the process that recorded the
    // terminal. Health is judged against the CURRENT boot only.
    "boot_id",
    "terminal_boot_id",
];

pub fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS rust_loop_health (
        loop_name TEXT PRIMARY KEY,
        wiring_status TEXT NOT NULL CHECK (wiring_status IN ('not_wired', 'ready')),
        generation INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0),
        terminal_generation INTEGER NOT NULL DEFAULT 0 CHECK (terminal_generation >= 0),
        terminal_status TEXT NOT NULL DEFAULT 'none' CHECK (terminal_status IN ('none','passed','error')),
        runtime_status TEXT NOT NULL DEFAULT 'active' CHECK (runtime_status IN ('active','suspended')),
        last_suspended_at INTEGER,
        last_suspension_reason TEXT,
        last_started_at INTEGER,
        last_passed_at INTEGER,
        last_error_at INTEGER,
        last_error TEXT,
        coalesced_total INTEGER NOT NULL DEFAULT 0 CHECK (coalesced_total >= 0),
        dropped_total INTEGER NOT NULL DEFAULT 0 CHECK (dropped_total >= 0),
        updated_at INTEGER NOT NULL,
        boot_id TEXT,
        terminal_boot_id TEXT
    );
    CREATE TABLE IF NOT EXISTS rust_boot_sessions (
        boot_id TEXT PRIMARY KEY,
        process_id INTEGER NOT NULL,
        source_commit TEXT,
        binary_sha256 TEXT,
        started_at INTEGER NOT NULL
    );",
    )
    .context("init rust_loop_health schema")?;
    let mut columns = conn.prepare("PRAGMA table_info(rust_loop_health)")?;
    let names = columns
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if names.iter().map(String::as_str).collect::<Vec<_>>() != CANONICAL_COLUMNS {
        bail!(
            "noncanonical rust_loop_health schema: expected {:?}, found {:?}; this unshipped table cannot be migrated without fabricating terminal evidence",
            CANONICAL_COLUMNS,
            names
        );
    }
    Ok(())
}

pub fn register_loop(conn: &Connection, id: LoopId, wiring: WiringStatus, now: i64) -> Result<()> {
    conn.execute(
        "INSERT INTO rust_loop_health (loop_name, wiring_status, updated_at) VALUES (?1, ?2, ?3)
         ON CONFLICT(loop_name) DO UPDATE SET
            wiring_status=excluded.wiring_status,
            runtime_status=CASE WHEN excluded.wiring_status='ready' THEN 'active' ELSE rust_loop_health.runtime_status END,
            updated_at=excluded.updated_at",
        params![id.as_str(), wiring.as_str(), now],
    )?;
    Ok(())
}

pub fn begin_loop_pass(conn: &Connection, id: LoopId, boot_id: &str, now: i64) -> Result<u64> {
    let tx = conn.unchecked_transaction()?;
    let state: Option<(String, String)> = tx
        .query_row(
            "SELECT wiring_status,runtime_status FROM rust_loop_health WHERE loop_name=?1",
            [id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    match state
        .as_ref()
        .map(|(wiring, runtime)| (wiring.as_str(), runtime.as_str()))
    {
        Some(("ready", "active")) => {}
        Some((wiring, runtime)) => bail!(
            "loop {} is wiring={wiring}, runtime={runtime}; refusing pass",
            id.as_str()
        ),
        None => bail!("loop {} is unregistered", id.as_str()),
    }
    tx.execute(
        "UPDATE rust_loop_health SET generation=generation+1,last_started_at=?2,updated_at=?2,boot_id=?3 WHERE loop_name=?1",
        params![id.as_str(), now, boot_id],
    )?;
    let generation: i64 = tx.query_row(
        "SELECT generation FROM rust_loop_health WHERE loop_name=?1",
        [id.as_str()],
        |row| row.get(0),
    )?;
    tx.commit()?;
    u64::try_from(generation).context("negative loop generation")
}

pub fn finish_loop_pass(
    conn: &Connection,
    id: LoopId,
    generation: u64,
    boot_id: &str,
    at: i64,
) -> Result<()> {
    terminal_update(conn, id, generation, boot_id, at, None)
}

pub fn fail_loop_pass(
    conn: &Connection,
    id: LoopId,
    generation: u64,
    boot_id: &str,
    at: i64,
    error: &str,
) -> Result<()> {
    terminal_update(
        conn,
        id,
        generation,
        boot_id,
        at,
        Some(&bounded_error(error)),
    )
}

fn terminal_update(
    conn: &Connection,
    id: LoopId,
    generation: u64,
    boot_id: &str,
    at: i64,
    error: Option<&str>,
) -> Result<()> {
    let generation = i64::try_from(generation)?;
    let changed = match error {
        None => conn.execute(
            "UPDATE rust_loop_health SET last_passed_at=?3,terminal_generation=?2,terminal_status='passed',updated_at=?3,terminal_boot_id=?4 WHERE loop_name=?1 AND generation=?2 AND terminal_generation < ?2",
            params![id.as_str(), generation, at, boot_id],
        )?,
        Some(error) => conn.execute(
            "UPDATE rust_loop_health SET last_error_at=?3,last_error=?4,terminal_generation=?2,terminal_status='error',updated_at=?3,terminal_boot_id=?5 WHERE loop_name=?1 AND generation=?2 AND terminal_generation < ?2",
            params![id.as_str(), generation, at, error, boot_id],
        )?,
    };
    if changed != 1 {
        bail!(
            "stale or already-terminal generation {generation} for loop {}",
            id.as_str()
        );
    }
    Ok(())
}

pub fn increment_loop_backpressure(
    conn: &Connection,
    id: LoopId,
    coalesced: u64,
    dropped: u64,
    now: i64,
) -> Result<()> {
    let changed = conn.execute(
        "UPDATE rust_loop_health SET coalesced_total=coalesced_total+?2,dropped_total=dropped_total+?3,updated_at=?4 WHERE loop_name=?1",
        params![id.as_str(), i64::try_from(coalesced)?, i64::try_from(dropped)?, now],
    )?;
    if changed != 1 {
        bail!("loop {} is unregistered", id.as_str());
    }
    Ok(())
}

pub fn suspend_loop(conn: &Connection, id: LoopId, at: i64, reason: &str) -> Result<()> {
    let changed = conn.execute(
        "UPDATE rust_loop_health SET runtime_status='suspended',last_suspended_at=?2,last_suspension_reason=?3,updated_at=?2 WHERE loop_name=?1",
        params![id.as_str(), at, bounded_error(reason)],
    )?;
    if changed != 1 {
        bail!("loop {} is unregistered", id.as_str());
    }
    Ok(())
}

pub fn reconcile_incomplete_on_restart(conn: &Connection, now: i64) -> Result<usize> {
    Ok(conn.execute(
        "UPDATE rust_loop_health SET last_error_at=?1,last_error='previous_generation_incomplete_on_restart',terminal_generation=generation,terminal_status='error',updated_at=?1 WHERE generation > terminal_generation",
        [now],
    )?)
}

pub fn list_loop_health(conn: &Connection) -> Result<Vec<LoopHealthRow>> {
    let mut stmt = conn.prepare(
        "SELECT loop_name,wiring_status,generation,terminal_generation,terminal_status,runtime_status,last_suspended_at,last_suspension_reason,last_started_at,last_passed_at,last_error_at,last_error,coalesced_total,dropped_total,updated_at,boot_id,terminal_boot_id FROM rust_loop_health ORDER BY CASE loop_name WHEN 'flow-analysis' THEN 0 WHEN 'fee-adjustment' THEN 1 WHEN 'rebalance-check' THEN 2 WHEN 'startup-snapshot' THEN 3 WHEN 'financial-snapshot' THEN 4 WHEN 'boltz-auto-cycle' THEN 5 WHEN 'capacity-planner' THEN 6 WHEN 'lnplus-watcher' THEN 7 ELSE 99 END",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
                row.get(9)?,
                row.get(10)?,
                row.get(11)?,
                row.get::<_, i64>(12)?,
                row.get::<_, i64>(13)?,
                row.get(14)?,
                row.get(15)?,
                row.get(16)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rows.into_iter()
        .map(
            |(
                id,
                wiring,
                generation,
                terminal_generation,
                terminal_status,
                runtime_status,
                suspended_at,
                suspension_reason,
                started,
                passed,
                error_at,
                error,
                coalesced,
                dropped,
                updated_at,
                boot_id,
                terminal_boot_id,
            )| {
                Ok(LoopHealthRow {
                    loop_id: LoopId::parse(&id)?,
                    wiring_status: WiringStatus::parse(&wiring)?,
                    generation: u64::try_from(generation)?,
                    terminal_generation: u64::try_from(terminal_generation)?,
                    terminal_status: TerminalStatus::parse(&terminal_status)?,
                    runtime_status: RuntimeStatus::parse(&runtime_status)?,
                    last_suspended_at: suspended_at,
                    last_suspension_reason: suspension_reason,
                    last_started_at: started,
                    last_passed_at: passed,
                    last_error_at: error_at,
                    last_error: error,
                    coalesced_total: u64::try_from(coalesced)?,
                    dropped_total: u64::try_from(dropped)?,
                    updated_at,
                    boot_id,
                    terminal_boot_id,
                })
            },
        )
        .collect()
}

fn bounded_error(error: &str) -> String {
    if error.len() <= MAX_ERROR_BYTES {
        return error.to_string();
    }
    let mut end = MAX_ERROR_BYTES;
    while !error.is_char_boundary(end) {
        end -= 1;
    }
    error[..end].to_string()
}

// ---------------------------------------------------------------------------
// Task 67: current-boot health binding
// ---------------------------------------------------------------------------

/// One loop's verdict FOR A GIVEN BOOT. The durable row is history; this
/// is the only thing a health surface may report as a current state.
///
/// `NeverRunThisBoot` is deliberately distinct from both `Passed` and
/// `Error`: a fresh process that has produced no evidence is not healthy
/// and is not broken -- it has not run. Collapsing it into either
/// direction is exactly the defect this type exists to prevent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootStatus {
    NotWired,
    Suspended,
    /// This boot began a pass that has not reached a terminal.
    Incomplete,
    Passed,
    Error,
    /// This process has produced no terminal for this loop. Any prior
    /// boot's terminal is history, never inherited.
    NeverRunThisBoot,
}

impl BootStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotWired => "not_wired",
            Self::Suspended => "suspended",
            Self::Incomplete => "incomplete",
            Self::Passed => "passed",
            Self::Error => "error",
            Self::NeverRunThisBoot => "never_run_this_boot",
        }
    }
}

/// Judge one durable row against the CURRENT boot.
pub fn current_boot_status(row: &LoopHealthRow, boot_id: &str) -> BootStatus {
    if row.wiring_status != WiringStatus::Ready {
        return BootStatus::NotWired;
    }
    if row.runtime_status == RuntimeStatus::Suspended {
        return BootStatus::Suspended;
    }
    // An in-flight generation counts only if THIS boot started it.
    if row.generation > row.terminal_generation {
        return if row.boot_id.as_deref() == Some(boot_id) {
            BootStatus::Incomplete
        } else {
            BootStatus::NeverRunThisBoot
        };
    }
    // A terminal counts only if THIS boot recorded it.
    if row.terminal_boot_id.as_deref() != Some(boot_id) {
        return BootStatus::NeverRunThisBoot;
    }
    match row.terminal_status {
        TerminalStatus::Passed => BootStatus::Passed,
        TerminalStatus::Error => BootStatus::Error,
        TerminalStatus::None => BootStatus::NeverRunThisBoot,
    }
}

/// Load one loop's durable row.
pub fn load_loop(conn: &Connection, id: LoopId) -> Result<Option<LoopHealthRow>> {
    Ok(list_loop_health(conn)?
        .into_iter()
        .find(|row| row.loop_id == id))
}

/// One process's identity. Minted once at startup and shared by every
/// loop (and, at cutover, by the fee restart markers) so the two cannot
/// drift apart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootIdentity {
    pub boot_id: String,
    pub process_id: i64,
    pub source_commit: Option<String>,
    pub binary_sha256: Option<String>,
    pub started_at: i64,
}

/// Record this boot's session. Idempotent: re-recording the same boot id
/// is not a second session.
pub fn record_boot_session(conn: &Connection, identity: &BootIdentity) -> Result<()> {
    conn.execute(
        "INSERT INTO rust_boot_sessions
             (boot_id, process_id, source_commit, binary_sha256, started_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(boot_id) DO NOTHING",
        params![
            identity.boot_id,
            identity.process_id,
            identity.source_commit,
            identity.binary_sha256,
            identity.started_at
        ],
    )
    .context("record boot session")?;
    Ok(())
}

/// The most recent boot sessions, newest first.
pub fn recent_boot_sessions(conn: &Connection, limit: i64) -> Result<Vec<BootIdentity>> {
    let mut stmt = conn
        .prepare(
            "SELECT boot_id, process_id, source_commit, binary_sha256, started_at
             FROM rust_boot_sessions ORDER BY started_at DESC, boot_id DESC LIMIT ?1",
        )
        .context("prepare boot sessions")?;
    let rows = stmt
        .query_map([limit], |row| {
            Ok(BootIdentity {
                boot_id: row.get(0)?,
                process_id: row.get(1)?,
                source_commit: row.get(2)?,
                binary_sha256: row.get(3)?,
                started_at: row.get(4)?,
            })
        })
        .context("query boot sessions")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("read boot sessions")?;
    Ok(rows)
}
