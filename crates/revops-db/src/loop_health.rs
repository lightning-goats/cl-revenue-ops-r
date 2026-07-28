use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};

pub const MAX_ERROR_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LoopId {
    Fee,
    Rebalance,
    Planner,
    LnPlus,
    Boltz,
}

pub const REQUIRED_LOOPS: [LoopId; 5] = [
    LoopId::Fee,
    LoopId::Rebalance,
    LoopId::Planner,
    LoopId::LnPlus,
    LoopId::Boltz,
];

impl LoopId {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fee => "fee",
            Self::Rebalance => "rebalance",
            Self::Planner => "planner",
            Self::LnPlus => "lnplus",
            Self::Boltz => "boltz",
        }
    }
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "fee" => Ok(Self::Fee),
            "rebalance" => Ok(Self::Rebalance),
            "planner" => Ok(Self::Planner),
            "lnplus" => Ok(Self::LnPlus),
            "boltz" => Ok(Self::Boltz),
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoopHealthRow {
    pub loop_id: LoopId,
    pub wiring_status: WiringStatus,
    pub generation: u64,
    pub terminal_generation: u64,
    pub terminal_status: TerminalStatus,
    pub last_started_at: Option<i64>,
    pub last_passed_at: Option<i64>,
    pub last_error_at: Option<i64>,
    pub last_error: Option<String>,
    pub coalesced_total: u64,
    pub dropped_total: u64,
    pub updated_at: i64,
}

impl LoopHealthRow {
    pub fn new(loop_id: LoopId, wiring_status: WiringStatus, updated_at: i64) -> Self {
        Self {
            loop_id,
            wiring_status,
            generation: 0,
            terminal_generation: 0,
            terminal_status: TerminalStatus::None,
            last_started_at: None,
            last_passed_at: None,
            last_error_at: None,
            last_error: None,
            coalesced_total: 0,
            dropped_total: 0,
            updated_at,
        }
    }
}

pub fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS rust_loop_health (
        loop_name TEXT PRIMARY KEY,
        wiring_status TEXT NOT NULL CHECK (wiring_status IN ('not_wired', 'ready')),
        generation INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0),
        terminal_generation INTEGER NOT NULL DEFAULT 0 CHECK (terminal_generation >= 0),
        terminal_status TEXT NOT NULL DEFAULT 'none' CHECK (terminal_status IN ('none','passed','error')),
        last_started_at INTEGER, last_passed_at INTEGER, last_error_at INTEGER, last_error TEXT,
        coalesced_total INTEGER NOT NULL DEFAULT 0 CHECK (coalesced_total >= 0),
        dropped_total INTEGER NOT NULL DEFAULT 0 CHECK (dropped_total >= 0), updated_at INTEGER NOT NULL);"
    ).context("init rust_loop_health schema")?;
    let mut columns = conn.prepare("PRAGMA table_info(rust_loop_health)")?;
    let names = columns
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if !names.iter().any(|name| name == "terminal_generation") {
        conn.execute_batch("ALTER TABLE rust_loop_health ADD COLUMN terminal_generation INTEGER NOT NULL DEFAULT 0 CHECK (terminal_generation >= 0)")?;
    }
    if !names.iter().any(|name| name == "terminal_status") {
        conn.execute_batch("ALTER TABLE rust_loop_health ADD COLUMN terminal_status TEXT NOT NULL DEFAULT 'none' CHECK (terminal_status IN ('none','passed','error'))")?;
    }
    Ok(())
}

pub fn register_loop(conn: &Connection, id: LoopId, wiring: WiringStatus, now: i64) -> Result<()> {
    conn.execute("INSERT INTO rust_loop_health (loop_name, wiring_status, updated_at) VALUES (?1, ?2, ?3)
        ON CONFLICT(loop_name) DO UPDATE SET wiring_status=excluded.wiring_status, updated_at=excluded.updated_at",
        params![id.as_str(), wiring.as_str(), now])?;
    Ok(())
}

pub fn begin_loop_pass(conn: &Connection, id: LoopId, now: i64) -> Result<u64> {
    let tx = conn.unchecked_transaction()?;
    let wiring: Option<String> = tx
        .query_row(
            "SELECT wiring_status FROM rust_loop_health WHERE loop_name=?1",
            [id.as_str()],
            |r| r.get(0),
        )
        .optional()?;
    match wiring.as_deref() {
        Some("ready") => {}
        Some(other) => bail!("loop {} is {other}, refusing pass", id.as_str()),
        None => bail!("loop {} is unregistered", id.as_str()),
    }
    tx.execute("UPDATE rust_loop_health SET generation=generation+1,last_started_at=?2,updated_at=?2 WHERE loop_name=?1", params![id.as_str(), now])?;
    let generation: i64 = tx.query_row(
        "SELECT generation FROM rust_loop_health WHERE loop_name=?1",
        [id.as_str()],
        |r| r.get(0),
    )?;
    tx.commit()?;
    u64::try_from(generation).context("negative loop generation")
}

pub fn finish_loop_pass(conn: &Connection, id: LoopId, generation: u64, at: i64) -> Result<()> {
    terminal_update(conn, id, generation, at, None)
}
pub fn fail_loop_pass(
    conn: &Connection,
    id: LoopId,
    generation: u64,
    at: i64,
    error: &str,
) -> Result<()> {
    terminal_update(conn, id, generation, at, Some(&bounded_error(error)))
}

fn terminal_update(
    conn: &Connection,
    id: LoopId,
    generation: u64,
    at: i64,
    error: Option<&str>,
) -> Result<()> {
    let generation = i64::try_from(generation)?;
    let changed = match error {
        None => conn.execute("UPDATE rust_loop_health SET last_passed_at=?3,terminal_generation=?2,terminal_status='passed',updated_at=?3 WHERE loop_name=?1 AND generation=?2", params![id.as_str(), generation, at])?,
        Some(error) => conn.execute("UPDATE rust_loop_health SET last_error_at=?3,last_error=?4,terminal_generation=?2,terminal_status='error',updated_at=?3 WHERE loop_name=?1 AND generation=?2", params![id.as_str(), generation, at, error])?,
    };
    if changed != 1 {
        bail!("stale generation {generation} for loop {}", id.as_str());
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
    let changed = conn.execute("UPDATE rust_loop_health SET coalesced_total=coalesced_total+?2,dropped_total=dropped_total+?3,updated_at=?4 WHERE loop_name=?1", params![id.as_str(), i64::try_from(coalesced)?, i64::try_from(dropped)?, now])?;
    if changed != 1 {
        bail!("loop {} is unregistered", id.as_str());
    }
    Ok(())
}

pub fn reconcile_incomplete_on_restart(conn: &Connection, now: i64) -> Result<usize> {
    Ok(conn.execute("UPDATE rust_loop_health SET last_error_at=?1,last_error='previous_generation_incomplete_on_restart',terminal_generation=generation,terminal_status='error',updated_at=?1 WHERE generation > terminal_generation", [now])?)
}

pub fn list_loop_health(conn: &Connection) -> Result<Vec<LoopHealthRow>> {
    let mut stmt = conn.prepare("SELECT loop_name,wiring_status,generation,terminal_generation,terminal_status,last_started_at,last_passed_at,last_error_at,last_error,coalesced_total,dropped_total,updated_at FROM rust_loop_health ORDER BY CASE loop_name WHEN 'fee' THEN 0 WHEN 'rebalance' THEN 1 WHEN 'planner' THEN 2 WHEN 'lnplus' THEN 3 WHEN 'boltz' THEN 4 ELSE 99 END")?;
    let rows = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, String>(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get(8)?,
                r.get::<_, i64>(9)?,
                r.get::<_, i64>(10)?,
                r.get(11)?,
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
                started,
                passed,
                error_at,
                error,
                coalesced,
                dropped,
                updated_at,
            )| {
                Ok(LoopHealthRow {
                    loop_id: LoopId::parse(&id)?,
                    wiring_status: WiringStatus::parse(&wiring)?,
                    generation: u64::try_from(generation)?,
                    terminal_generation: u64::try_from(terminal_generation)?,
                    terminal_status: TerminalStatus::parse(&terminal_status)?,
                    last_started_at: started,
                    last_passed_at: passed,
                    last_error_at: error_at,
                    last_error: error,
                    coalesced_total: u64::try_from(coalesced)?,
                    dropped_total: u64::try_from(dropped)?,
                    updated_at,
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
