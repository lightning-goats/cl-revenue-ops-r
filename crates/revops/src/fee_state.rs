//! State lifecycle for the dry-run fee cycle (Phase 4b Task 4):
//! re-hydrate-per-cycle [`rehydrate`] + the production-DB-safe
//! [`JournalStateSink`].
//!
//! ## Design Note 1 (`docs/superpowers/plans/2026-07-17-phase4b-wiring.md`)
//!
//! The DECIDED lifecycle for the whole dry-run window is
//! **re-hydrate-per-cycle**: both controllers start every cycle from
//! Python's persisted `v2_state_json` flush, so every cycle is an
//! independent parity trial instead of a seed-once run whose in-memory
//! state diverges from Python's broadcast-driven trajectory from cycle 2
//! onward. At cutover this flips to seed-once (hydrate ONCE at start, then
//! evolve in memory with `StateSink` pointing at the Rust-owned writable
//! DB) — a scheduler config change, not a rework; a `StateLifecycle` enum
//! carrying that flip belongs to T6 (the scheduler), not this module.
//!
//! [`rehydrate`] reuses the exact functions Phase 4 Task 9's production
//! gate proved byte-exact over 40/40 real `fee_strategy_state` blobs
//! (`revops_fees::state_store::{read_fee_strategy_rows, parse_v2_blob,
//! load_fee_state, load_cycle_state}`) — this module adds no new parsing
//! logic, only the DB-read -> fresh-map-swap plumbing around them.
//!
//! ## `StateSink` never points at the production DB
//!
//! Per the plan's Global Constraints ("Python stays authoritative... any
//! new write target must be a Rust-owned file next to
//! `revops-r-observer.db`"), [`JournalStateSink`] holds no DB connection at
//! all — its only state is a file path, and it serializes what WOULD be
//! flushed into a JSONL file in the dry-run journal directory for offline
//! comparison (`tools/diff-harness`), mirroring `revops_fees::journal`'s
//! `Journal` (decisions) with a state-focused sibling file.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use revops_fees::cycle::{
    serialize_cycle_state_payload, ChannelCycleState, ChannelFeeState, ControllerState,
    SkipGateEpoch, StateSink,
};
use revops_fees::pyjson::{dumps_python, OValue};
use revops_fees::pyrand::DecisionInputError;
use revops_fees::state_store::{
    fee_state_to_v2_dict, load_cycle_state, load_fee_state, parse_v2_blob, read_fee_strategy_rows,
    FeeStrategyRow,
};

/// Default journal file name under the dry-run journal directory.
pub const STATE_JOURNAL_FILE_NAME: &str = "fee_dryrun_state.jsonl";

/// Design Note 1: called at the top of EVERY dry-run cycle. Vegas state and
/// `vegas_wake_armed` are process-lifetime (Python keeps them as module
/// globals, not in `v2_state_json`) — hydration REPLACES
/// `cycle_states`/`fee_states` and PRESERVES `state.vegas` /
/// `state.vegas_wake_armed` / `last_decision_summary`.
///
/// Builds fresh maps from `read_fee_strategy_rows(conn)` (one row per
/// channel currently persisted) rather than mutating the existing maps in
/// place, so a channel that disappeared from the DB since the last cycle
/// (or was never persisted) does not linger as stale in-memory state.
pub fn rehydrate(state: &mut ControllerState, conn: &rusqlite::Connection) {
    let rows = read_fee_strategy_rows(conn);
    hydrate_from_strategy_rows(state, &rows);
}

/// The body of [`rehydrate`], factored so the SeedOnce one-time import
/// ([`seed_once_from_python`]) can hydrate from rows it has ALREADY read
/// (and strictly validated) without a second production-DB read.
fn hydrate_from_strategy_rows(state: &mut ControllerState, rows: &[FeeStrategyRow]) {
    let mut cycle_states = std::collections::BTreeMap::new();
    let mut fee_states = std::collections::BTreeMap::new();

    for row in rows {
        let env = parse_v2_blob(&row.v2_state_json, row);
        let fee_state = load_fee_state(&env, row);
        let cycle_state = load_cycle_state(&env, row);
        fee_states.insert(row.channel_id.clone(), fee_state);
        cycle_states.insert(row.channel_id.clone(), cycle_state);
    }

    // Phase 4b Task 8b (Design Note 1 addendum): maintain the skip gate's
    // cross-cycle memory. This cycle's FRESH hydration is exactly the epoch
    // Python's NEXT-cycle skip gate will be conditioned on, so record it as
    // `skip_gate_seen` and PROMOTE the previous cycle's `seen` into
    // `skip_gate_prev` -- the value the gate reads THIS cycle. Rust's own
    // last observation, not the just-flushed blob, is the pre-decision epoch
    // (the freshly-flushed `last_update` is what Python just WROTE for the
    // cycle Rust is reproducing -- the wrong epoch; see the fee-window
    // diagnosis, H1). Built from the fresh `cycle_states` BEFORE they move
    // into `state`; a channel absent from `skip_gate_prev` next cycle is a
    // bootstrap / first appearance (the gate then falls back to live state
    // and flags the channel non-comparable).
    let this_cycle_seen: std::collections::BTreeMap<String, SkipGateEpoch> = cycle_states
        .iter()
        .map(|(id, c)| {
            (
                id.clone(),
                SkipGateEpoch {
                    last_update: c.last_update,
                    is_sleeping: c.is_sleeping,
                },
            )
        })
        .collect();
    state.skip_gate_prev = std::mem::replace(&mut state.skip_gate_seen, this_cycle_seen);

    state.cycle_states = cycle_states;
    state.fee_states = fee_states;
}

// ---------------------------------------------------------------------------
// Restart-persistent SeedOnce (stateful-shadow Task 5, amendments R5/R6)
// ---------------------------------------------------------------------------

/// Where a `SeedOnce` scheduler's state came from at startup (Task 5 step
/// 2). Once a Rust generation exists, Python is NEVER an autonomous-state
/// source again -- `PythonSeed` can only ever happen with an empty
/// Rust-owned store (generation 0).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HydrationSource {
    /// One-time cold-start import from Python's `fee_strategy_state`
    /// snapshot (read-only), through the exact `from_dict` parity path
    /// with the fail-closed refusal rules of [`seed_once_from_python`].
    PythonSeed,
    /// Restart: hydrated from the Rust-owned store's recorded generation.
    RustGeneration(u64),
}

impl HydrationSource {
    /// The `rust_fee_restart_markers.hydration_source` column encoding.
    pub fn label(&self) -> String {
        match self {
            HydrationSource::PythonSeed => "python_seed".to_string(),
            HydrationSource::RustGeneration(generation) => {
                format!("rust_generation:{generation}")
            }
        }
    }
}

/// Hydrate `ControllerState` from Rust-OWNED [`FeeStateRow`]s (a restart
/// under `SeedOnce`). Unlike [`rehydrate`] over Python rows, this is
/// STRICT: Rust wrote these envelopes itself ([`serialize_state_envelope`]),
/// so an unparseable blob or a missing `fee_state`/`cycle_state` key is
/// corruption, not legacy drift -- the whole hydration fails closed
/// (`Err`, state untouched) rather than silently defaulting any channel.
///
/// On success the skip-gate epoch caches (`skip_gate_prev`/`skip_gate_seen`)
/// are set to the hydrated epochs: under SeedOnce Rust owns the state, so
/// the pre-decision epoch the gate consumes IS the owned `last_update`
/// (amendment R5) -- there is no Python post-decision flush to correct for.
pub fn rehydrate_from_rows(
    state: &mut ControllerState,
    rows: &[revops_db::fee_runway::FeeStateRow],
) -> Result<(), DecisionInputError> {
    use revops_fees::pyjson::parse;

    // Validate EVERY row before touching `state` (no partial hydration).
    let mut strategy_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let parsed = parse(&row.v2_state_json).map_err(|e| {
            DecisionInputError::new(format!(
                "corrupt Rust-owned state row for channel {}: {e}",
                row.channel_id
            ))
        })?;
        let has_envelope_keys = matches!(parsed.get("fee_state"), Some(v) if v.as_obj().is_some())
            && matches!(parsed.get("cycle_state"), Some(v) if v.as_obj().is_some());
        if !has_envelope_keys {
            return Err(DecisionInputError::new(format!(
                "corrupt Rust-owned state row for channel {}: missing fee_state/cycle_state \
                 envelope keys Rust always writes",
                row.channel_id
            )));
        }
        strategy_rows.push(FeeStrategyRow {
            channel_id: row.channel_id.clone(),
            last_update: row.last_update,
            v2_state_json: row.v2_state_json.clone(),
            ..FeeStrategyRow::default()
        });
    }

    hydrate_from_strategy_rows(state, &strategy_rows);
    set_skip_gates_to_owned(state);
    Ok(())
}

/// Rust owns the state: pre-decision epoch caches == owned epochs
/// (amendment R5). Called after every SeedOnce hydration and at the top of
/// every SeedOnce cycle by the scheduler.
pub fn set_skip_gates_to_owned(state: &mut ControllerState) {
    let epochs: std::collections::BTreeMap<String, SkipGateEpoch> = state
        .cycle_states
        .iter()
        .map(|(id, c)| {
            (
                id.clone(),
                SkipGateEpoch {
                    last_update: c.last_update,
                    is_sleeping: c.is_sleeping,
                },
            )
        })
        .collect();
    state.skip_gate_seen = epochs.clone();
    state.skip_gate_prev = epochs;
}

/// sha256 (hex) of the canonical seed-payload serialization: rows sorted
/// by `channel_id`, each contributing `channel_id`, `v2_state_json`, and
/// `last_update` separated by newlines. Recorded in the seed event's
/// provenance so a later audit can prove exactly what was imported.
pub fn seed_payload_sha256(rows: &[FeeStrategyRow]) -> String {
    use sha2::{Digest, Sha256};
    let mut sorted: Vec<&FeeStrategyRow> = rows.iter().collect();
    sorted.sort_by(|a, b| a.channel_id.cmp(&b.channel_id));
    let mut hasher = Sha256::new();
    for row in sorted {
        hasher.update(row.channel_id.as_bytes());
        hasher.update(b"\n");
        hasher.update(row.v2_state_json.as_bytes());
        hasher.update(b"\n");
        hasher.update(row.last_update.to_string().as_bytes());
        hasher.update(b"\n");
    }
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// What the one-time seed attempt produced (Task R6): either the state was
/// hydrated and a `seeded` provenance event should be recorded, or the
/// WHOLE snapshot was refused (state untouched) and a `seed_refused` event
/// should be recorded. There is no partial-seed variant on purpose.
#[derive(Debug)]
pub enum SeedOutcome {
    Seeded(revops_db::fee_runway::FeeSeedEventRow),
    Refused(revops_db::fee_runway::FeeSeedEventRow),
}

/// The one-time cold-start seed from Python's `fee_strategy_state`
/// (READ-ONLY -- `conn` is the per-cycle evidence snapshot's pinned
/// read-only connection; nothing here writes anywhere).
///
/// Fail-closed rule (revision plan Task R6): every row is checked through
/// the EXACT `from_dict` parity classes
/// ([`revops_fees::state_store::seed_parity_violation`]) BEFORE any state
/// is touched. Any field where Python's own `from_dict` would raise
/// (non-numeric `_last_fee_min`, string rows inside `posterior_precision`,
/// dict entries in `posterior_bias`, ...) refuses the WHOLE seed: the
/// offending channel+field are logged and returned in the `seed_refused`
/// event row, and the controller state is left untouched -- no partial
/// seed, no silent fresh-state fallback.
pub fn seed_once_from_python(
    state: &mut ControllerState,
    conn: &rusqlite::Connection,
    source_db_path: &str,
    now: i64,
    source_commit: &str,
) -> SeedOutcome {
    use revops_db::fee_runway::FeeSeedEventRow;
    use revops_fees::state_store::seed_parity_violation;

    let rows = read_fee_strategy_rows(conn);
    let base = FeeSeedEventRow {
        seeded_at: now,
        outcome: "seeded".to_string(),
        source_db_path: source_db_path.to_string(),
        source_max_last_update: rows.iter().map(|r| r.last_update).max().unwrap_or(0),
        row_count: rows.len() as i64,
        payload_sha256: seed_payload_sha256(&rows),
        source_commit: source_commit.to_string(),
        refused_channel: None,
        refused_field: None,
        detail: None,
    };

    for row in &rows {
        let env = parse_v2_blob(&row.v2_state_json, row);
        if let Some(violation) = seed_parity_violation(&env) {
            eprintln!(
                "revops: SEED REFUSED (fail closed, staying passive-observer): channel {} \
                 field {}: {} -- no partial seed, no fresh-state fallback",
                row.channel_id, violation.field, violation.detail
            );
            return SeedOutcome::Refused(FeeSeedEventRow {
                outcome: "seed_refused".to_string(),
                refused_channel: Some(row.channel_id.clone()),
                refused_field: Some(violation.field),
                detail: Some(violation.detail),
                ..base
            });
        }
    }

    hydrate_from_strategy_rows(state, &rows);
    set_skip_gates_to_owned(state);
    SeedOutcome::Seeded(base)
}

/// The Rust-owned state store as the `SeedOnce` scheduler sees it, from
/// its plain `std::thread` (hence the blocking shape). The production
/// implementation is [`revops_db::owner::ObserverHandle`] (the observer-db
/// single-owner actor: writable, Rust-owned, structurally never the
/// production DB); tests substitute a direct-connection double that calls
/// the same `revops_db::fee_runway` functions.
pub trait RunwayStateStore: Send {
    fn load_latest_state(&self) -> anyhow::Result<revops_db::fee_runway::FeeStateSnapshot>;
    fn commit_fee_cycle(
        &self,
        commit: revops_db::fee_runway::FeeCycleCommit,
    ) -> anyhow::Result<u64>;
    fn record_seed_event(
        &self,
        event: revops_db::fee_runway::FeeSeedEventRow,
    ) -> anyhow::Result<i64>;
    fn record_restart_marker(
        &self,
        marker: revops_db::fee_runway::FeeRestartMarkerRow,
    ) -> anyhow::Result<i64>;
}

impl RunwayStateStore for revops_db::owner::ObserverHandle {
    fn load_latest_state(&self) -> anyhow::Result<revops_db::fee_runway::FeeStateSnapshot> {
        self.blocking_load_latest_fee_state()
    }

    fn commit_fee_cycle(
        &self,
        commit: revops_db::fee_runway::FeeCycleCommit,
    ) -> anyhow::Result<u64> {
        self.blocking_commit_fee_cycle(commit)
    }

    fn record_seed_event(
        &self,
        event: revops_db::fee_runway::FeeSeedEventRow,
    ) -> anyhow::Result<i64> {
        self.blocking_record_fee_seed_event(event)
    }

    fn record_restart_marker(
        &self,
        marker: revops_db::fee_runway::FeeRestartMarkerRow,
    ) -> anyhow::Result<i64> {
        self.blocking_record_fee_restart_marker(marker)
    }
}

/// One flushed channel's would-be persisted envelope, in the same
/// top-level shape production's `v2_state_json` carries
/// (`algorithm_version`/`fee_state`/`cycle_state`/the 3 shared scalars) —
/// built directly from the caller's fresh cycle/fee state via
/// `fee_state_to_v2_dict`/`serialize_cycle_state_payload`, with no
/// production-DB read involved (unlike `state_store::build_merged_row`,
/// which reconciles against a previously-persisted envelope; this sink has
/// no such envelope to reconcile against, and none of the callers of this
/// module need byte-identical merge-fidelity — only a faithful record of
/// what this cycle's states looked like).
fn state_envelope(cycle: &ChannelCycleState, fee: &ChannelFeeState) -> OValue {
    OValue::obj(vec![
        (
            "algorithm_version".to_string(),
            OValue::str(fee.algorithm_version.clone()),
        ),
        ("fee_state".to_string(), fee_state_to_v2_dict(fee)),
        (
            "cycle_state".to_string(),
            serialize_cycle_state_payload(cycle),
        ),
        (
            "last_gossip_refresh".to_string(),
            OValue::Int(fee.last_gossip_refresh()),
        ),
        (
            "last_broadcast_at".to_string(),
            OValue::Int(fee.last_broadcast_at()),
        ),
        (
            "dynamic_htlcmin_baseline_msat".to_string(),
            fee.dynamic_htlcmin_baseline_msat()
                .map(OValue::Int)
                .unwrap_or(OValue::Null),
        ),
    ])
}

/// The serialized envelope string a Rust-owned `rust_fee_state.v2_state_json`
/// column carries -- [`state_envelope`] rendered through `dumps_python`,
/// i.e. exactly what [`JournalStateSink`] writes per row. The SeedOnce
/// commit path uses this to build [`revops_db::fee_runway::FeeStateRow`]s,
/// and [`rehydrate_from_rows`] requires its envelope keys on the way back.
pub fn serialize_state_envelope(cycle: &ChannelCycleState, fee: &ChannelFeeState) -> String {
    dumps_python(&state_envelope(cycle, fee))
}

/// `StateSink` that never touches the production DB: serializes each
/// flushed row as one JSONL line `{"channel_id":..., "v2_state_json":...}`
/// into `<journal_dir>/fee_dryrun_state.jsonl` for offline comparison. The
/// only state this type holds is a file path — it never opens a
/// `rusqlite::Connection`, so it is structurally incapable of reaching
/// `revops-r-db-path` / `econ_ledger.db`.
#[derive(Debug)]
pub struct JournalStateSink {
    path: PathBuf,
    transaction_lock: Arc<Mutex<()>>,
}

static JOURNAL_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>> = OnceLock::new();
static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn transaction_lock(path: &Path) -> Arc<Mutex<()>> {
    let registry = JOURNAL_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(path.to_path_buf(), Arc::downgrade(&lock));
    lock
}

#[derive(Debug)]
struct OwnedStaging {
    path: PathBuf,
    file: Option<File>,
    remove_on_drop: bool,
}

impl OwnedStaging {
    fn create(destination: &Path) -> std::io::Result<Self> {
        let parent = destination.parent().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "journal has no parent")
        })?;
        let name = destination
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid journal name")
            })?;
        for _ in 0..64 {
            let sequence = STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!(".{name}.{}.{}.tmp", std::process::id(), sequence));
            match OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&path)
            {
                Ok(file) => {
                    return Ok(Self {
                        path,
                        file: Some(file),
                        remove_on_drop: true,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not allocate unique journal staging file",
        ))
    }

    fn file_mut(&mut self) -> &mut File {
        self.file.as_mut().expect("staging file is open")
    }

    fn close(&mut self) {
        self.file.take();
    }

    fn preserve(mut self) {
        self.remove_on_drop = false;
    }
}

impl Drop for OwnedStaging {
    fn drop(&mut self) {
        self.file.take();
        if self.remove_on_drop {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl JournalStateSink {
    /// State journal inside `dir` under the frozen file name.
    pub fn open_dir(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let canonical_dir = std::fs::canonicalize(dir)?;
        let path = canonical_dir.join(STATE_JOURNAL_FILE_NAME);
        Ok(JournalStateSink {
            transaction_lock: transaction_lock(&path),
            path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl StateSink for JournalStateSink {
    fn flush_batch(
        &self,
        rows: &[(String, ChannelCycleState, ChannelFeeState)],
    ) -> Result<(), DecisionInputError> {
        self.flush_batch_with_rename(rows, |from, to| std::fs::rename(from, to))
    }
}

impl JournalStateSink {
    /// Persist one complete next journal image through an exclusively-owned,
    /// same-directory staging file, then atomically replace the prior artifact.
    fn flush_batch_with_rename<F>(
        &self,
        rows: &[(String, ChannelCycleState, ChannelFeeState)],
        rename: F,
    ) -> Result<(), DecisionInputError>
    where
        F: FnOnce(&Path, &Path) -> std::io::Result<()>,
    {
        if rows.is_empty() {
            return Ok(());
        }

        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let (mut next, prior_permissions) = match std::fs::read(&self.path) {
            Ok(bytes) => {
                let permissions = std::fs::metadata(&self.path)
                    .map_err(|error| {
                        DecisionInputError::new(format!(
                            "state journal metadata failed ({}): {error}",
                            self.path.display()
                        ))
                    })?
                    .permissions();
                (bytes, Some(permissions))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (Vec::new(), None),
            Err(error) => {
                return Err(DecisionInputError::new(format!(
                    "state journal read failed ({}): {error}",
                    self.path.display()
                )));
            }
        };
        for (channel_id, cycle, fee) in rows {
            let envelope = state_envelope(cycle, fee);
            let line = OValue::obj(vec![
                ("channel_id".to_string(), OValue::str(channel_id.clone())),
                (
                    "v2_state_json".to_string(),
                    OValue::str(dumps_python(&envelope)),
                ),
            ]);
            next.extend_from_slice(dumps_python(&line).as_bytes());
            next.push(b'\n');
        }

        let mut staging = OwnedStaging::create(&self.path).map_err(|error| {
            DecisionInputError::new(format!(
                "state journal staging open failed ({}): {error}",
                self.path.display()
            ))
        })?;
        staging.file_mut().write_all(&next).map_err(|error| {
            DecisionInputError::new(format!(
                "state journal staging write failed ({}): {error}",
                staging.path.display()
            ))
        })?;
        if let Some(permissions) = prior_permissions {
            staging
                .file_mut()
                .set_permissions(permissions)
                .map_err(|error| {
                    DecisionInputError::new(format!(
                        "state journal staging permissions failed ({}): {error}",
                        staging.path.display()
                    ))
                })?;
        }
        staging.file_mut().sync_all().map_err(|error| {
            DecisionInputError::new(format!(
                "state journal staging sync failed ({}): {error}",
                staging.path.display()
            ))
        })?;
        staging.close();

        rename(&staging.path, &self.path).map_err(|error| {
            DecisionInputError::new(format!(
                "state journal atomic rename failed ({} -> {}): {error}",
                staging.path.display(),
                self.path.display()
            ))
        })?;
        staging.preserve();
        Ok(())
    }
}

#[cfg(test)]
mod journal_atomic_tests {
    use super::*;
    use revops_fees::cycle::StateSink;

    #[test]
    fn injected_rename_failure_cleans_owned_staging_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let sink = JournalStateSink::open_dir(tmp.path()).expect("sink");
        let prior = b"{\"prior\":\"complete\"}\n";
        std::fs::write(sink.path(), prior).expect("seed journal");

        let error = sink
            .flush_batch_with_rename(
                &[(
                    "chan_a".to_string(),
                    ChannelCycleState::default(),
                    ChannelFeeState::default(),
                )],
                |_, _| Err(std::io::Error::other("injected rename failure")),
            )
            .expect_err("rename failure must propagate");
        assert!(error.to_string().contains("injected rename failure"));
        assert_eq!(std::fs::read(sink.path()).unwrap(), prior);

        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                name.starts_with(".fee_dryrun_state.jsonl.") && name.ends_with(".tmp")
            })
            .collect();
        assert!(
            leftovers.is_empty(),
            "rename failure left owned staging files: {leftovers:?}"
        );

        sink.flush_batch(&[(
            "chan_b".to_string(),
            ChannelCycleState::default(),
            ChannelFeeState::default(),
        )])
        .expect("later flush is not poisoned");
    }
}
