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

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use revops_fees::cycle::{
    serialize_cycle_state_payload, ChannelCycleState, ChannelFeeState, ControllerState, StateSink,
};
use revops_fees::pyjson::{dumps_python, OValue};
use revops_fees::state_store::{
    fee_state_to_v2_dict, load_cycle_state, load_fee_state, parse_v2_blob, read_fee_strategy_rows,
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
    let mut cycle_states = std::collections::BTreeMap::new();
    let mut fee_states = std::collections::BTreeMap::new();

    for row in &rows {
        let env = parse_v2_blob(&row.v2_state_json, row);
        let fee_state = load_fee_state(&env, row);
        let cycle_state = load_cycle_state(&env, row);
        fee_states.insert(row.channel_id.clone(), fee_state);
        cycle_states.insert(row.channel_id.clone(), cycle_state);
    }

    state.cycle_states = cycle_states;
    state.fee_states = fee_states;
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

/// `StateSink` that never touches the production DB: serializes each
/// flushed row as one JSONL line `{"channel_id":..., "v2_state_json":...}`
/// into `<journal_dir>/fee_dryrun_state.jsonl` for offline comparison. The
/// only state this type holds is a file path — it never opens a
/// `rusqlite::Connection`, so it is structurally incapable of reaching
/// `revops-r-db-path` / `econ_ledger.db`.
#[derive(Debug)]
pub struct JournalStateSink {
    path: PathBuf,
}

impl JournalStateSink {
    /// State journal inside `dir` under the frozen file name.
    pub fn open_dir(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(JournalStateSink {
            path: dir.join(STATE_JOURNAL_FILE_NAME),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl StateSink for JournalStateSink {
    /// `_flush_pending_fee_strategy_rows`'s dry-run analogue (py
    /// 4030-4058): ONE flush per cycle, appended as one JSONL line per row.
    fn flush_batch(&self, rows: &[(String, ChannelCycleState, ChannelFeeState)]) {
        if rows.is_empty() {
            return;
        }
        let mut buf = String::new();
        for (channel_id, cycle, fee) in rows {
            let envelope = state_envelope(cycle, fee);
            let line = OValue::obj(vec![
                ("channel_id".to_string(), OValue::str(channel_id.clone())),
                (
                    "v2_state_json".to_string(),
                    OValue::str(dumps_python(&envelope)),
                ),
            ]);
            buf.push_str(&dumps_python(&line));
            buf.push('\n');
        }
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .unwrap_or_else(|e| panic!("open state journal {}: {e}", self.path.display()));
        f.write_all(buf.as_bytes())
            .unwrap_or_else(|e| panic!("write state journal {}: {e}", self.path.display()));
    }
}
