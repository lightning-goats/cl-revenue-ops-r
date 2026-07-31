//! Pure response builders for `revenue-econ-reconcile`.
//!
//! Port of `revenue_econ_reconcile` (cl-revenue-ops.py:6099-6148). The
//! actual reconciliation math (`econ_reconcile.reconcile`/`.apply`/
//! `.fee_intent_completeness`) is fully ported and reused verbatim from
//! `revops_econ::reconcile` -- this module only shapes an already-computed
//! [`ReconciliationReport`] (plus the separately-computed
//! `fee_intent_completeness` blob and optional `apply` count) into the
//! exact Python response dict, plus (Task 66 slice 4)
//! [`econ_reconcile_response`], the full assembly `main.rs` registers:
//! availability arms, the DB truth-map read, the reconcile sweep, the
//! best-effort completeness cross-check, and the `apply=true` corrections
//! — appended ONLY to the Rust-owned dry-run ledger (see
//! [`EconReconcileSources::ledger_path`]).

use revops_db::actor::DbHandle;
use revops_econ::ledger::EconLedger;
use revops_econ::reconcile::{DbReservationState, ReconciliationReport};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::rpc_params::{is_truthy_py, python_int};

/// py `apply: bool = False` under pyln's permissive binding: generic
/// truthiness, like every other handler-defined bool in this port.
pub fn parse_apply(raw: Option<&Value>) -> bool {
    raw.map(is_truthy_py).unwrap_or(false)
}

/// py `stale_after_seconds: int = 3600`, floored at 60 inside the handler
/// (`max(60, int(stale_after_seconds))`, cl-revenue-ops.py:6114).
pub fn parse_stale_after_seconds(raw: Option<&Value>) -> Result<i64, Value> {
    let default = Value::Number(3600.into());
    let parsed = python_int(raw.unwrap_or(&default)).map_err(|error| json!({"error": error}))?;
    Ok(parsed.max(60))
}

/// Everything the assembled response needs. `ledger_path` is the
/// RUST-OWNED dry-run ledger (`<journal_dir>/econ_ledger_dryrun.db`, the
/// same file `GovernorWiring` writes) — NEVER Python's production
/// `econ_ledger.db`, which stays Python-owned until cutover. `apply=true`
/// therefore appends reconciliation events only to this port's own
/// evidence trail, exactly as the governed fee path already does.
pub struct EconReconcileSources<'a> {
    /// `config_resolve::econ_shadow_enabled` outcome — a FAILED read is
    /// not "disabled"; it takes Python's outer error arm.
    pub enabled: Result<bool, String>,
    pub db: Option<&'a DbHandle>,
    pub ledger_path: Option<PathBuf>,
    pub apply: bool,
    /// Already [`parse_stale_after_seconds`]-floored.
    pub stale_after_seconds: i64,
    pub now: i64,
}

/// Port of `revenue_econ_reconcile`'s arm structure
/// (cl-revenue-ops.py:6095-6148): disabled hint, ledger/database
/// unavailability, the dry-run report, the best-effort completeness
/// cross-check (its OWN inner catch), and the optional apply count. Every
/// remaining failure is Python's outer `{enabled: true, error}` arm.
pub async fn econ_reconcile_response(s: EconReconcileSources<'_>) -> Value {
    match s.enabled {
        Ok(true) => {}
        Ok(false) => return build_econ_reconcile_disabled(),
        Err(error) => return build_econ_reconcile_error(&error),
    }
    let (Some(handle), Some(ledger_path)) = (s.db, s.ledger_path.as_ref()) else {
        return build_econ_reconcile_unavailable();
    };
    // py `ledger_for_reconciliation()` returns None on an open failure ->
    // the same unavailable arm.
    let Ok(ledger) = EconLedger::open(ledger_path) else {
        return build_econ_reconcile_unavailable();
    };

    let db_states: BTreeMap<String, DbReservationState> =
        match revops_db::queries::spend_reservation_states(handle).await {
            Ok(states) => states
                .into_iter()
                .map(|(key, state)| {
                    (
                        key,
                        DbReservationState {
                            status: state.status,
                            reserved_sats: state.reserved_sats,
                        },
                    )
                })
                .collect(),
            Err(error) => return build_econ_reconcile_error(&error.to_string()),
        };

    let report = match revops_econ::reconcile::reconcile(
        &ledger,
        &db_states,
        s.now,
        s.stale_after_seconds,
    ) {
        Ok(report) => report,
        Err(error) => return build_econ_reconcile_error(&error.to_string()),
    };

    // py wraps BOTH the fee-changes read and the completeness math in one
    // inner try/except -> the {"status": "error"} blob, never a whole-RPC
    // failure (cl-revenue-ops.py:6134-6142).
    let completeness = match revops_db::queries::recent_fee_change_timestamps(handle, 500).await {
        Ok(timestamps) => {
            let rows: Vec<Value> = timestamps
                .into_iter()
                .map(|timestamp| json!({"timestamp": timestamp}))
                .collect();
            match revops_econ::reconcile::fee_intent_completeness(&ledger, &rows, s.now, 86400, 120)
            {
                Ok(value) => value,
                Err(error) => json!({"status": "error", "error": error.to_string()}),
            }
        }
        Err(error) => json!({"status": "error", "error": error.to_string()}),
    };

    let applied = if s.apply {
        match revops_econ::reconcile::apply(&ledger, &report, s.now) {
            Ok(count) => Some(count),
            Err(error) => return build_econ_reconcile_error(&error.to_string()),
        }
    } else {
        None
    };

    build_econ_reconcile(&report, completeness, applied)
}

/// Port of the `econ_shadow is None or not econ_shadow.enabled()` branch
/// (cl-revenue-ops.py:6108-6110).
pub fn build_econ_reconcile_disabled() -> Value {
    json!({
        "enabled": false,
        "hint": "revenue-config set econ_shadow_enabled true",
    })
}

/// Port of the `ledger is None or database is None` branch
/// (cl-revenue-ops.py:6112-6113).
pub fn build_econ_reconcile_unavailable() -> Value {
    json!({
        "enabled": true,
        "error": "ledger or database unavailable",
    })
}

/// Port of the generic `except Exception as e` branch
/// (cl-revenue-ops.py:6147-6148).
pub fn build_econ_reconcile_error(message: &str) -> Value {
    json!({
        "enabled": true,
        "error": message,
    })
}

/// Port of the success path (cl-revenue-ops.py:6118-6146).
///
/// `fee_intent_completeness` must already be either
/// `econ_reconcile.fee_intent_completeness(...)`'s success shape or the
/// `{"status": "error", "error": str(e)}` fallback Python builds on its own
/// try/except (py 6140-6142) -- both are just `Value` blobs here, matching
/// Python's identical treatment of the two cases (no `Result` at this
/// boundary since Python swallows the exception into the same dict shape).
/// `applied` is `Some(count)` only when the caller ran with `apply=true`
/// and actually called `econ_reconcile::apply` (py 6143-6145) -- `None`
/// omits the `"applied"` key entirely, matching Python's `if apply:` guard
/// (the key is absent, not null, in the dry-run response).
pub fn build_econ_reconcile(
    report: &ReconciliationReport,
    fee_intent_completeness: Value,
    applied: Option<usize>,
) -> Value {
    let divergences: Vec<Value> = report
        .divergences
        .iter()
        .map(|d| {
            json!({
                "kind": d.kind,
                "key": d.key,
                "ledger_reserved_msat": d.ledger_reserved_msat,
                "db_status": d.db_status,
                "db_reserved_sats": d.db_reserved_sats,
                "quarantined": d.resolution.is_none(),
                "details": d.details,
            })
        })
        .collect();

    let mut out = json!({
        "enabled": true,
        "checked": report.checked,
        "matched": report.matched,
        "divergences": divergences,
        "fee_intent_completeness": fee_intent_completeness,
    });
    if let Some(applied) = applied {
        out["applied"] = json!(applied);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use revops_econ::reconcile::Divergence;

    #[test]
    fn parse_apply_is_python_truthiness_defaulting_false() {
        assert!(!parse_apply(None));
        assert!(!parse_apply(Some(&json!(false))));
        assert!(!parse_apply(Some(&json!(0))));
        assert!(parse_apply(Some(&json!(true))));
        // Python truthiness: a non-empty string is true — including "false".
        assert!(parse_apply(Some(&json!("false"))));
    }

    #[test]
    fn parse_stale_after_floors_at_sixty_and_defaults_3600() {
        assert_eq!(parse_stale_after_seconds(None).unwrap(), 3600);
        assert_eq!(parse_stale_after_seconds(Some(&json!(7200))).unwrap(), 7200);
        assert_eq!(
            parse_stale_after_seconds(Some(&json!(5))).unwrap(),
            60,
            "py max(60, ...)"
        );
        assert_eq!(parse_stale_after_seconds(Some(&json!("90"))).unwrap(), 90);
        assert!(
            parse_stale_after_seconds(Some(&json!("junk"))).unwrap_err()["error"]
                .as_str()
                .is_some()
        );
    }

    #[test]
    fn disabled_and_unavailable_branches_match_python_shapes() {
        let disabled = build_econ_reconcile_disabled();
        assert_eq!(disabled["enabled"], false);
        assert_eq!(
            disabled["hint"],
            "revenue-config set econ_shadow_enabled true"
        );

        let error = build_econ_reconcile_error("boom");
        assert_eq!(error["enabled"], true);
        assert_eq!(error["error"], "boom");

        let unavailable = build_econ_reconcile_unavailable();
        assert_eq!(unavailable["enabled"], true);
        assert_eq!(unavailable["error"], "ledger or database unavailable");
    }

    #[test]
    fn quarantined_reflects_resolution_none_not_a_separate_flag() {
        let report = ReconciliationReport {
            checked: 2,
            matched: 0,
            divergences: vec![
                Divergence {
                    kind: "db_missing".to_string(),
                    key: "k1".to_string(),
                    ledger_reserved_msat: 5000,
                    db_status: None,
                    db_reserved_sats: None,
                    resolution: Some(json!({"reserved_msat": 0})),
                    details: json!({}),
                },
                Divergence {
                    kind: "unknown_outcome".to_string(),
                    key: "k2".to_string(),
                    ledger_reserved_msat: 1000,
                    db_status: Some("active".to_string()),
                    db_reserved_sats: Some(1),
                    resolution: None,
                    details: json!({"reason_code": "EXTERNAL_OUTCOME_UNKNOWN"}),
                },
            ],
        };
        let v = build_econ_reconcile(&report, json!({"status": "no_intent_data"}), None);
        assert_eq!(v["checked"], 2);
        assert_eq!(v["divergences"][0]["quarantined"], false);
        assert_eq!(v["divergences"][1]["quarantined"], true);
        // Control: dry-run (apply=None) must omit the "applied" key entirely.
        assert!(v.get("applied").is_none());
    }

    #[test]
    fn applied_count_is_present_only_when_supplied() {
        let report = ReconciliationReport {
            checked: 0,
            matched: 0,
            divergences: vec![],
        };
        let v = build_econ_reconcile(&report, json!({"status": "ok"}), Some(3));
        assert_eq!(v["applied"], 3);
    }
}
