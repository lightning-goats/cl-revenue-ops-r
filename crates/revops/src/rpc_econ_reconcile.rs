//! Pure response builders for `revenue-econ-reconcile`.
//!
//! Port of `revenue_econ_reconcile` (cl-revenue-ops.py:6099-6148). The
//! actual reconciliation math (`econ_reconcile.reconcile`/`.apply`/
//! `.fee_intent_completeness`) is fully ported and reused verbatim from
//! `revops_econ::reconcile` -- this module only shapes an already-computed
//! [`ReconciliationReport`] (plus the separately-computed
//! `fee_intent_completeness` blob and optional `apply` count) into the
//! exact Python response dict. `econ_shadow`/`ledger`/`database`
//! availability and the `apply=true` execution itself are the caller's
//! job (py 6108-6117, 6143-6145): this crate does no I/O and commits no
//! ledger events.

use revops_econ::reconcile::ReconciliationReport;
use serde_json::{json, Value};

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
