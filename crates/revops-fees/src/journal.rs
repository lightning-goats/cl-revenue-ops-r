//! JSONL decision journal: `FeeDecision` + full reason traces, appended
//! per dry-run cycle for `tools/diff-harness/diff_fee_decisions.py`.
//!
//! One line per decision, `pyjson::dumps_python`-serialized (Python
//! `json.dumps` default separators, floats as `repr`) so the diff
//! instrument reads exactly what a Python consumer would have written.
//!
//! Journal shape contract (Task 10 plan): `algorithm_values` is the
//! Python-parity `FeeAdjustment.algorithm_values` dict, CONTENT-identical
//! to production (the T11 diff instrument compares it and the `reason`
//! string against production's recorded reasons — the reason STRING format
//! is the wire contract, see `cycle.rs::build_reason`). `trace` carries
//! the dry-run superset diagnostics (skip reason, gossip-gate disposition,
//! floor/ceiling terms, htlcmax, governed decision) — together
//! `algorithm_values ∪ trace` is the plan's "superset of Python's
//! algorithm_values + last_decision_summary".

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::execution::GovernedTrace;
use crate::pyjson::{dumps_python, OValue};
use crate::replay_wire::WireValue;

/// Default journal file name under the Rust plugin's DB directory.
pub const JOURNAL_FILE_NAME: &str = "fee_dryrun_journal.jsonl";

/// One dry-run fee decision (a would-be `FeeAdjustment` broadcast, a
/// suppressed target, or a scheduler skip) with its full reason trace.
#[derive(Debug, Clone, PartialEq)]
pub struct FeeDecision {
    pub channel_id: String,
    pub peer_id: String,
    pub old_fee_ppm: i64,
    pub new_fee_ppm: i64,
    /// Frozen wire-contract reason string (py `FeeAdjustment.reason`).
    pub reason: String,
    /// Frozen `FeeReasonCode` wire value, or a `skip_*` classification.
    pub reason_code: String,
    /// Python-parity `algorithm_values` (py `FeeAdjustment` 2426-2435);
    /// `OValue::Null` for skip decisions (Python emits no adjustment).
    pub algorithm_values: OValue,
    /// Dry-run superset diagnostics (NOT part of the Python wire shape).
    pub trace: OValue,
    /// What `set_channel_fee` WOULD have done (dry-run).
    pub would_broadcast: bool,
    pub governed: Option<GovernedTrace>,
    pub cycle_id: String,
    pub at: i64,
}

impl FeeDecision {
    /// Canonical replay-wire representation used for exact capture comparison.
    pub fn to_replay_wire(&self) -> WireValue {
        fn convert(value: &OValue) -> WireValue {
            match value {
                OValue::Null => WireValue::Null,
                OValue::Bool(value) => WireValue::Bool(*value),
                OValue::Int(value) => WireValue::Integer(*value),
                OValue::Float(value) => {
                    WireValue::TaggedFloat(revops_econ::pyfloat::py_repr(*value))
                }
                OValue::Str(value) => WireValue::String(value.clone()),
                OValue::Arr(items) => WireValue::Array(items.iter().map(convert).collect()),
                OValue::Obj(entries) => WireValue::Object(
                    entries
                        .iter()
                        .map(|(key, value)| (key.clone(), convert(value)))
                        .collect(),
                ),
            }
        }

        convert(&self.to_ovalue())
    }

    /// Ordered wire object; key order frozen for journal readers.
    pub fn to_ovalue(&self) -> OValue {
        let governed = match &self.governed {
            None => OValue::Null,
            Some(g) => OValue::obj(vec![
                ("authorized".to_string(), OValue::Bool(g.authorized)),
                (
                    "reason_code".to_string(),
                    OValue::str(g.reason_code.clone()),
                ),
                ("intent_id".to_string(), OValue::str(g.intent_id.clone())),
                (
                    "idempotency_key".to_string(),
                    OValue::str(g.idempotency_key.clone()),
                ),
            ]),
        };
        OValue::obj(vec![
            (
                "channel_id".to_string(),
                OValue::str(self.channel_id.clone()),
            ),
            ("peer_id".to_string(), OValue::str(self.peer_id.clone())),
            ("old_fee_ppm".to_string(), OValue::Int(self.old_fee_ppm)),
            ("new_fee_ppm".to_string(), OValue::Int(self.new_fee_ppm)),
            ("reason".to_string(), OValue::str(self.reason.clone())),
            (
                "reason_code".to_string(),
                OValue::str(self.reason_code.clone()),
            ),
            (
                "algorithm_values".to_string(),
                self.algorithm_values.clone(),
            ),
            ("trace".to_string(), self.trace.clone()),
            (
                "would_broadcast".to_string(),
                OValue::Bool(self.would_broadcast),
            ),
            ("governed".to_string(), governed),
            ("cycle_id".to_string(), OValue::str(self.cycle_id.clone())),
            ("at".to_string(), OValue::Int(self.at)),
        ])
    }

    /// One JSONL line (no trailing newline).
    pub fn to_jsonl_line(&self) -> String {
        dumps_python(&self.to_ovalue())
    }
}

/// Append-only JSONL journal at `<rust-db-dir>/fee_dryrun_journal.jsonl`.
#[derive(Debug)]
pub struct Journal {
    path: PathBuf,
}

impl Journal {
    /// Journal inside `db_dir` under the frozen file name.
    pub fn open_dir(db_dir: &Path) -> std::io::Result<Journal> {
        std::fs::create_dir_all(db_dir)?;
        Ok(Journal {
            path: db_dir.join(JOURNAL_FILE_NAME),
        })
    }

    /// Journal at an explicit file path.
    pub fn at_path(path: impl Into<PathBuf>) -> Journal {
        Journal { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn appender(&self) -> std::io::Result<File> {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
    }

    /// Append one decision as one JSONL line.
    pub fn append(&self, decision: &FeeDecision) -> std::io::Result<()> {
        let mut f = self.appender()?;
        f.write_all(decision.to_jsonl_line().as_bytes())?;
        f.write_all(b"\n")?;
        Ok(())
    }

    /// Append a whole cycle's decisions in one open/write/close.
    pub fn append_all(&self, decisions: &[FeeDecision]) -> std::io::Result<()> {
        if decisions.is_empty() {
            return Ok(());
        }
        let mut f = self.appender()?;
        let mut buf = String::new();
        for d in decisions {
            buf.push_str(&d.to_jsonl_line());
            buf.push('\n');
        }
        f.write_all(buf.as_bytes())?;
        Ok(())
    }
}
