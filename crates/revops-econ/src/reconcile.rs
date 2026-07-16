//! Ledger <-> DB reconciliation (port of `modules/econ_reconcile.py`).
//!
//! Compares econ-ledger replay state against the production
//! `spend_reservations` truth and classifies divergences. The LEDGER
//! reconciles TO the DB — the DB remains the authorization authority until
//! Phase 2 completes; resolutions are new append-only
//! `reconciliation_completed` events, never DB writes (spec: "corrections
//! are new events").
//!
//! Ambiguous execution outcomes (`execution_started` with no terminal event
//! beyond the staleness horizon) are QUARANTINED — reported with reason
//! code `EXTERNAL_OUTCOME_UNKNOWN` and never auto-resolved (spec
//! reservation machine: "on ambiguous execution outcome, retain/quarantine
//! the reservation until reconciled").

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Value};

use crate::ledger::EconLedger;
use crate::types::{EconError, EconResult};

/// DB-side truth for one `spend_reservations` row, as read by the caller.
pub struct DbReservationState {
    pub status: String,
    pub reserved_sats: i64,
}

/// Statuses in `spend_reservations` that mean "no longer outstanding".
const DB_TERMINAL: [&str; 2] = ["spent", "released"];

/// Terminal ledger events for the "started without terminal" scan. NOTE:
/// deliberately a different set than `ledger::EVENT_TYPES`'s
/// `TERMINAL_EVENTS` — this mirrors Python's `_started_without_terminal`
/// exactly, which does NOT include `execution_outcome_unknown` (an
/// already-resolved-elsewhere terminal state a reconciliation sweep has no
/// business re-quarantining).
const STARTED_TERMINAL_EVENTS: [&str; 5] = [
    "execution_succeeded",
    "execution_failed",
    "intent_rejected",
    "intent_deferred",
    "reconciliation_completed",
];

/// One classified ledger/DB divergence for a single idempotency key.
#[derive(Debug, Clone, PartialEq)]
pub struct Divergence {
    pub kind: String,
    pub key: String,
    pub ledger_reserved_msat: i64,
    pub db_status: Option<String>,
    pub db_reserved_sats: Option<i64>,
    /// Resolution to append as a `reconciliation_completed` event's
    /// amounts. `None` = quarantined (unknown outcome), never
    /// auto-resolved.
    pub resolution: Option<Value>,
    pub details: Value,
}

/// The result of one reconciliation sweep.
#[derive(Debug, Clone, PartialEq)]
pub struct ReconciliationReport {
    pub checked: usize,
    pub matched: usize,
    pub divergences: Vec<Divergence>,
}

/// `idempotency_key -> latest execution_started timestamp`, for keys with
/// no terminal event (mirrors Python's `_started_without_terminal`).
fn started_without_terminal(ledger: &EconLedger) -> EconResult<BTreeMap<String, i64>> {
    let mut started: BTreeMap<String, i64> = BTreeMap::new();
    let mut terminal_keys: BTreeSet<String> = BTreeSet::new();
    for event in ledger.events(0)? {
        let key = &event.idempotency_key;
        if event.event_type == "execution_started" {
            let entry = started.entry(key.clone()).or_insert(event.at);
            if event.at > *entry {
                *entry = event.at;
            }
        } else if STARTED_TERMINAL_EVENTS.contains(&event.event_type.as_str()) {
            terminal_keys.insert(key.clone());
        }
    }
    started.retain(|k, _| !terminal_keys.contains(k));
    Ok(started)
}

/// Runs one reconciliation sweep. See the module doc and the four
/// resolvable divergence kinds (`db_missing`, `ledger_stale_reservation`,
/// `ledger_missing_reservation`, `amount_mismatch`) plus the quarantined
/// `unknown_outcome` kind (never auto-resolved). Keys are iterated sorted
/// for determinism (both maps here are `BTreeMap`/`BTreeSet`, so this falls
/// out of the collection choice rather than an explicit sort step).
pub fn reconcile(
    ledger: &EconLedger,
    db_states: &BTreeMap<String, DbReservationState>,
    now: i64,
    stale_after_seconds: i64,
) -> EconResult<ReconciliationReport> {
    let state = ledger.replay()?;
    let ledger_outstanding = state.reserved_msat;

    // Spec reservation machine: an execution with no terminal outcome
    // RETAINS its reservation until reconciled — such keys are excluded
    // from resolvable classification entirely (fresh in-flight is normal;
    // stale in-flight surfaces below as quarantined unknown_outcome, never
    // auto-resolved).
    let in_flight = started_without_terminal(ledger)?;

    let mut keys: BTreeSet<String> = ledger_outstanding.keys().cloned().collect();
    for (key, row) in db_states {
        if row.status == "active" {
            keys.insert(key.clone());
        }
    }
    for key in in_flight.keys() {
        keys.remove(key);
    }

    let mut divergences: Vec<Divergence> = Vec::new();
    let mut matched = 0usize;

    for key in &keys {
        let ledger_msat = *ledger_outstanding.get(key).unwrap_or(&0);
        let db_row = db_states.get(key);

        match db_row {
            None => divergences.push(Divergence {
                kind: "db_missing".to_string(),
                key: key.clone(),
                ledger_reserved_msat: ledger_msat,
                db_status: None,
                db_reserved_sats: None,
                resolution: Some(json!({"reserved_msat": 0})),
                details: json!({"note": "no spend_reservations row"}),
            }),
            Some(row) if DB_TERMINAL.contains(&row.status.as_str()) && ledger_msat > 0 => {
                divergences.push(Divergence {
                    kind: "ledger_stale_reservation".to_string(),
                    key: key.clone(),
                    ledger_reserved_msat: ledger_msat,
                    db_status: Some(row.status.clone()),
                    db_reserved_sats: Some(row.reserved_sats),
                    resolution: Some(json!({"reserved_msat": 0})),
                    details: json!({"db_status": row.status, "terminal": true}),
                })
            }
            Some(row) if row.status == "active" && ledger_msat == 0 => {
                divergences.push(Divergence {
                    kind: "ledger_missing_reservation".to_string(),
                    key: key.clone(),
                    ledger_reserved_msat: 0,
                    db_status: Some(row.status.clone()),
                    db_reserved_sats: Some(row.reserved_sats),
                    resolution: Some(json!({"reserved_msat": row.reserved_sats * 1000})),
                    details: json!({"db_status": row.status}),
                })
            }
            Some(row) if row.status == "active" && ledger_msat != row.reserved_sats * 1000 => {
                divergences.push(Divergence {
                    kind: "amount_mismatch".to_string(),
                    key: key.clone(),
                    ledger_reserved_msat: ledger_msat,
                    db_status: Some(row.status.clone()),
                    db_reserved_sats: Some(row.reserved_sats),
                    resolution: Some(json!({"reserved_msat": row.reserved_sats * 1000})),
                    details: json!({"db_status": row.status}),
                })
            }
            _ => matched += 1,
        }
    }

    for (key, started_at) in &in_flight {
        if now - started_at > stale_after_seconds {
            let db_row = db_states.get(key);
            divergences.push(Divergence {
                kind: "unknown_outcome".to_string(),
                key: key.clone(),
                ledger_reserved_msat: *ledger_outstanding.get(key).unwrap_or(&0),
                db_status: db_row.map(|r| r.status.clone()),
                db_reserved_sats: db_row.map(|r| r.reserved_sats),
                resolution: None, // quarantine — human/executor reconciles
                details: json!({
                    "reason_code": "EXTERNAL_OUTCOME_UNKNOWN",
                    "started_at": started_at,
                    "age_seconds": now - started_at,
                }),
            });
        }
    }

    Ok(ReconciliationReport {
        checked: keys.len(),
        matched,
        divergences,
    })
}

/// Compare authoritative `fee_changes` rows against ledgered fee intents
/// per cycle (the manual cross-check that exposed the 2026-07-12
/// thread-affinity capture loss, automated).
///
/// Only cycles AFTER the first ledgered fee intent are judged — pre-shadow
/// history is out of scope. Cycle timestamps are matched with a tolerance
/// because the journal stamp and the `fee_changes` rows are written seconds
/// apart within one cycle; a single cycle's rows can also land across
/// adjacent seconds (observed live 2026-07-12: 3 rows at `:41` + 5 rows at
/// `:42` for one 8-change cycle) — change timestamps are clustered within
/// `tolerance_seconds` BEFORE comparison, or fragments false-positive
/// against the whole cycle's intent count.
pub fn fee_intent_completeness(
    ledger: &EconLedger,
    fee_changes: &[Value],
    now: i64,
    window_seconds: i64,
    tolerance_seconds: i64,
) -> EconResult<Value> {
    let mut intents_by_ts: BTreeMap<i64, i64> = BTreeMap::new();
    for event in ledger.events(0)? {
        if event.event_type != "intent_proposed" {
            continue;
        }
        let cycle = &event.cycle_id;
        // fee-cycle-<ts>: post-hoc batch recording (observe mode);
        // fee-broadcast-<ts>: per-broadcast governed recording (2H).
        if !(cycle.starts_with("fee-cycle-") || cycle.starts_with("fee-broadcast-")) {
            continue;
        }
        let Some((_, ts_str)) = cycle.rsplit_once('-') else {
            continue;
        };
        let Ok(ts) = ts_str.parse::<i64>() else {
            continue;
        };
        *intents_by_ts.entry(ts).or_insert(0) += 1;
    }

    if intents_by_ts.is_empty() {
        return Ok(json!({
            "status": "no_intent_data",
            "cycles_checked": 0,
            "complete": Value::Null,
            "mismatched_cycles": {},
        }));
    }

    let min_intent_ts = *intents_by_ts
        .keys()
        .next()
        .expect("checked non-empty above");
    let window_start = (now - window_seconds).max(min_intent_ts);

    let mut changes_by_ts: BTreeMap<i64, i64> = BTreeMap::new();
    for row in fee_changes {
        let ts = row.get("timestamp").and_then(Value::as_i64).unwrap_or(0);
        if ts >= window_start {
            *changes_by_ts.entry(ts).or_insert(0) += 1;
        }
    }

    // Cluster change timestamps within tolerance into cycles before
    // comparing against ledgered intent counts.
    let mut clusters: Vec<(i64, i64, i64)> = Vec::new(); // (start_ts, end_ts, change_count)
    for (&ts, &count) in &changes_by_ts {
        let extend = matches!(clusters.last(), Some(&(_, end, _)) if ts - end <= tolerance_seconds);
        if extend {
            let last = clusters.last_mut().expect("checked Some above");
            last.1 = ts;
            last.2 += count;
        } else {
            clusters.push((ts, ts, count));
        }
    }

    let mut mismatched = serde_json::Map::new();
    for &(start_ts, end_ts, change_count) in &clusters {
        let matched_intents: i64 = intents_by_ts
            .iter()
            .filter(|(&intent_ts, _)| {
                start_ts - tolerance_seconds <= intent_ts && intent_ts <= end_ts + tolerance_seconds
            })
            .map(|(_, &count)| count)
            .sum();
        if matched_intents != change_count {
            mismatched.insert(
                start_ts.to_string(),
                json!({"fee_changes": change_count, "intents": matched_intents}),
            );
        }
    }

    Ok(json!({
        "status": "ok",
        "window_start": window_start,
        "cycles_checked": clusters.len(),
        "complete": mismatched.is_empty(),
        "mismatched_cycles": Value::Object(mismatched),
    }))
}

/// Converts a `{"field": <int>, ...}` JSON object into checked ledger
/// amounts. Fails closed (never coerces or drops) if a field is missing an
/// integer value — this port's money paths carry no f64 ingress at all, so
/// any non-integer here would indicate a construction bug upstream, not
/// legitimate data.
fn value_to_amounts(v: &Value) -> EconResult<BTreeMap<String, i64>> {
    let obj = v.as_object().ok_or_else(|| EconError {
        msg: "reconcile: resolution must be a JSON object".to_string(),
    })?;
    let mut out = BTreeMap::new();
    for (k, val) in obj {
        let n = val.as_i64().ok_or_else(|| EconError {
            msg: format!("reconcile: resolution field {k:?} is not an integer"),
        })?;
        out.insert(k.clone(), n);
    }
    Ok(out)
}

/// Appends one `reconciliation_completed` event per RESOLVABLE divergence
/// (quarantined `unknown_outcome`s are skipped). Returns the number
/// applied. `intent_id = key[:16]` (or `key` if shorter); `cycle_id =
/// "reconcile"`.
pub fn apply(ledger: &EconLedger, report: &ReconciliationReport, now: i64) -> EconResult<usize> {
    let mut applied = 0usize;
    for divergence in &report.divergences {
        let Some(resolution) = &divergence.resolution else {
            continue;
        };
        let mut details = divergence.details.clone();
        match details.as_object_mut() {
            Some(map) => {
                map.insert("kind".to_string(), Value::String(divergence.kind.clone()));
            }
            None => {
                return Err(EconError {
                    msg: format!(
                        "reconcile: divergence details for {:?} is not a JSON object",
                        divergence.key
                    ),
                })
            }
        }
        let amounts = value_to_amounts(resolution)?;
        let intent_id: String = divergence.key.chars().take(16).collect();

        ledger.append(
            "reconciliation_completed",
            &intent_id,
            &divergence.key,
            "reconcile",
            now,
            &amounts,
            &details,
        )?;
        applied += 1;
    }
    Ok(applied)
}
