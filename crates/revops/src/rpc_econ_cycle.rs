//! Assembly for `revenue-econ-cycle`, the READ-ONLY Workstream H shadow
//! cycle diagnostic.
//!
//! Port of `revenue_econ_cycle` (cl-revenue-ops.py:6150-6174) plus
//! `modules/econ_cycle.run_shadow_cycle` (econ_cycle.py:140-185) — the
//! live collector the `revops_econ::cycle` module doc deferred as "Phase
//! 2b wiring". The deterministic core (`plan_cycle`, batch arbitration,
//! `CycleResult::to_wire`) is reused verbatim from `revops_econ`.
//!
//! Two deliberate runtime facts, both Python-parity:
//!
//! - **Candidates come from the rebalance engine**, which in this port is
//!   unassembled until Task 69's authority-gated construction (see
//!   `rebalance_owner::RebalanceOwnerDeps::engine`). `main.rs` therefore
//!   passes [`EconCycleSources::candidates`] `= None`, which is exactly
//!   Python's `engine is None` arm (`"rebalance engine unavailable"`,
//!   py 6163-6164). The full cycle path below is driven by tests with
//!   injected pairs until the engine assembles.
//! - **Ledgering goes to the RUST-OWNED dry-run ledger only**
//!   (`<journal_dir>/econ_ledger_dryrun.db`, the same file `GovernorWiring`
//!   and `revenue-econ-reconcile` use) — never Python's production
//!   `econ_ledger.db`, which stays Python-owned until cutover.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};

use revops_econ::context::CycleContext;
use revops_econ::cycle::{plan_cycle, RebalancePair};
use revops_econ::ledger::EconLedger;
use revops_econ::types::UnixTime;
use serde_json::{json, Value};

// The disabled/error dicts are byte-identical to `revenue-econ-reconcile`'s
// (py 6158-6160 vs 6108-6110, 6173-6174 vs 6147-6148) — reuse those
// builders rather than duplicating the literals.
use crate::rpc_econ_reconcile::{build_econ_reconcile_disabled, build_econ_reconcile_error};

/// Port of `run_shadow_cycle` (econ_cycle.py:140-185): one collection
/// pass over already-fetched candidates, pure intent generation, batch
/// arbitration, best-effort `intent_proposed` ledgering, wire result.
/// Fail-open: returns `None` instead of raising into the caller (py
/// 184-185); per-intent ledger append failures are swallowed (py
/// 181-182), and a `None` ledger skips ledgering entirely while still
/// returning the wire result (py 168-169).
pub fn run_shadow_cycle(
    pairs: &[RebalancePair],
    ledger: Option<&EconLedger>,
    now: i64,
    cycle_seq: i64,
) -> Option<Value> {
    let cycle_id = format!("econ-cycle-{now}-{cycle_seq}");
    let cycle_time = UnixTime::new(now).ok()?;
    // py 154-165: a base context with seed=0 exists only to derive the
    // real seed deterministically from the cycle identity.
    let base_ctx = CycleContext::new(cycle_id.clone(), cycle_time, 0, cycle_id.clone()).ok()?;
    let seed = base_ctx.derive_seed("econ-cycle").ok()?;
    let ctx = CycleContext::new(cycle_id.clone(), cycle_time, seed, cycle_id.clone()).ok()?;

    let result = plan_cycle(pairs, &ctx, pairs.len() as i64).ok()?;

    if let Some(ledger) = ledger {
        let no_amounts = BTreeMap::new();
        for env in &result.intents {
            // py 170-182: each append is individually best-effort.
            let details = json!({
                "target": env.target,
                "shadow_cycle": true,
                "explanation": env.explanation.render(),
            });
            let _ = ledger.append(
                "intent_proposed",
                env.intent_id.as_str(),
                &env.idempotency_key,
                &cycle_id,
                now,
                &no_amounts,
                &details,
            );
        }
    }
    Some(result.to_wire())
}

/// Everything the assembled response needs. See the module doc for why
/// `candidates` is `None` at the production call site pre-Task-69 and why
/// `ledger_path` is the Rust-owned dry-run ledger.
pub struct EconCycleSources {
    /// `config_resolve::econ_shadow_enabled` outcome — a FAILED read is
    /// not "disabled"; it takes Python's outer error arm.
    pub enabled: Result<bool, String>,
    /// `None` = no engine (py `engine is None`, cl-revenue-ops.py:
    /// 6161-6164). `Some(Err(_))` = the candidate fetch itself failed,
    /// which in Python raises inside `run_shadow_cycle`'s try and
    /// fail-opens to the `"shadow cycle failed open"` arm (py 6170-6171).
    pub candidates: Option<Result<Vec<RebalancePair>, String>>,
    /// The RUST-OWNED dry-run ledger path. `EconLedger::open` failure
    /// mirrors py `ledger_for_reconciliation()` returning `None`: the
    /// cycle still runs and returns, it just isn't ledgered.
    pub ledger_path: Option<PathBuf>,
    pub now: i64,
}

/// Port of `revenue_econ_cycle`'s arm structure (cl-revenue-ops.py:
/// 6150-6174). `seq` is the port of the `_econ_cycle_seq` module global
/// (py 6147): incremented ONLY after the enabled and engine gates pass
/// (py increments at 6166, inside both checks), so disabled or
/// engine-unavailable calls never consume a sequence number.
pub fn econ_cycle_response(s: EconCycleSources, seq: &AtomicI64) -> Value {
    match s.enabled {
        Ok(true) => {}
        Ok(false) => return build_econ_reconcile_disabled(),
        Err(error) => return build_econ_reconcile_error(&error),
    }
    let Some(candidates) = s.candidates else {
        return build_econ_reconcile_error("rebalance engine unavailable");
    };
    // py 6166: `_econ_cycle_seq += 1`, then the new value is used.
    let cycle_seq = seq.fetch_add(1, Ordering::SeqCst) + 1;
    // py 152: `pairs = rebalance_engine.find_candidates() or []`; an
    // exception there is caught by run_shadow_cycle's own try -> None.
    let wire = match candidates {
        Ok(pairs) => {
            let ledger = s
                .ledger_path
                .as_ref()
                .and_then(|path| EconLedger::open(path).ok());
            run_shadow_cycle(&pairs, ledger.as_ref(), s.now, cycle_seq)
        }
        Err(_) => None,
    };
    match wire {
        Some(cycle) => json!({"enabled": true, "cycle": cycle}),
        None => build_econ_reconcile_error("shadow cycle failed open"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_and_error_arms_share_reconciles_exact_shapes() {
        let seq = AtomicI64::new(0);
        let disabled = econ_cycle_response(
            EconCycleSources {
                enabled: Ok(false),
                candidates: None,
                ledger_path: None,
                now: 1_700_000_000,
            },
            &seq,
        );
        assert_eq!(disabled["enabled"], false);
        assert_eq!(
            disabled["hint"],
            "revenue-config set econ_shadow_enabled true"
        );

        let errored = econ_cycle_response(
            EconCycleSources {
                enabled: Err("boom".to_string()),
                candidates: None,
                ledger_path: None,
                now: 1_700_000_000,
            },
            &seq,
        );
        assert_eq!(errored["enabled"], true);
        assert_eq!(errored["error"], "boom");
        assert_eq!(
            seq.load(Ordering::SeqCst),
            0,
            "py increments _econ_cycle_seq only after both gates pass"
        );
    }
}
