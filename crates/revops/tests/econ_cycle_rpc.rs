//! Integration tests for `revenue-econ-cycle`'s production assembly
//! (`rpc_econ_cycle::econ_cycle_response`, the exact function `main.rs`
//! registers) driven with injected candidate pairs and a REAL temp
//! ledger file -- the same seams the runtime wires, minus the rebalance
//! engine (unassembled until Task 69; the runtime arm for that is pinned
//! by the e2e test in `manifest.rs`).

use std::sync::atomic::{AtomicI64, Ordering};

use revops::rpc_econ_cycle::{econ_cycle_response, EconCycleSources};
use revops_econ::context::CycleContext;
use revops_econ::cycle::RebalancePair;
use revops_econ::ledger::EconLedger;
use revops_econ::types::UnixTime;

const NOW: i64 = 1_753_900_000;

fn pair(src: &str, dst: &str, amount_sats: i64, score: f64) -> RebalancePair {
    RebalancePair {
        source_channel_id: src.to_string(),
        dest_channel_id: dst.to_string(),
        amount_sats,
        pair_budget_sats: amount_sats * 2,
        score,
        score_decomposition: None,
    }
}

/// py 6161-6164: no engine -> the error dict, and `_econ_cycle_seq` is
/// NOT consumed (py increments at 6166, after the check).
#[test]
fn engine_unavailable_arm_leaves_the_sequence_untouched() {
    let seq = AtomicI64::new(0);
    let v = econ_cycle_response(
        EconCycleSources {
            enabled: Ok(true),
            candidates: None,
            ledger_path: None,
            now: NOW,
        },
        &seq,
    );
    assert_eq!(v["enabled"], true);
    assert_eq!(v["error"], "rebalance engine unavailable");
    assert_eq!(seq.load(Ordering::SeqCst), 0);
}

/// py: `find_candidates()` raising is caught by `run_shadow_cycle`'s own
/// try (econ_cycle.py:149, 184-185) -> None -> the fail-open arm
/// (cl-revenue-ops.py:6170-6171). The sequence number WAS consumed
/// (py 6166 runs before the call).
#[test]
fn candidate_fetch_failure_fail_opens_after_consuming_a_seq() {
    let seq = AtomicI64::new(0);
    let v = econ_cycle_response(
        EconCycleSources {
            enabled: Ok(true),
            candidates: Some(Err("rpc timeout".to_string())),
            ledger_path: None,
            now: NOW,
        },
        &seq,
    );
    assert_eq!(v["enabled"], true);
    assert_eq!(v["error"], "shadow cycle failed open");
    assert_eq!(
        seq.load(Ordering::SeqCst),
        1,
        "py increments before the run"
    );
}

/// The full success path: deterministic context derivation, plan, wire
/// shape, and one `intent_proposed` ledger row per PROPOSED intent in the
/// Rust-owned dry-run ledger with Python's exact details keys.
#[test]
fn full_cycle_ledgers_proposed_intents_into_the_dryrun_ledger() {
    let dir = tempfile::tempdir().unwrap();
    let ledger_path = dir.path().join("econ_ledger_dryrun.db");
    let seq = AtomicI64::new(0);

    let v = econ_cycle_response(
        EconCycleSources {
            enabled: Ok(true),
            candidates: Some(Ok(vec![
                pair("100x1x0", "200x1x0", 50_000, 0.8),
                pair("300x1x0", "400x1x0", 25_000, 0.4),
            ])),
            ledger_path: Some(ledger_path.clone()),
            now: NOW,
        },
        &seq,
    );

    assert_eq!(v["enabled"], true);
    let cycle = &v["cycle"];
    let cycle_id = format!("econ-cycle-{NOW}-1");
    assert_eq!(cycle["cycle_id"], cycle_id.as_str());
    assert_eq!(cycle["snapshot_id"], cycle_id.as_str());
    assert_eq!(cycle["cycle_time"], NOW);
    assert_eq!(cycle["channel_count"], 2);
    assert_eq!(cycle["intents_proposed"], 2);
    assert_eq!(cycle["schema_name"], "econ_cycle_result");

    // py 154-165: the seed derives from a seed-0 base context over the
    // cycle identity -- recompute it independently.
    let base = CycleContext::new(
        cycle_id.clone(),
        UnixTime::new(NOW).unwrap(),
        0,
        cycle_id.clone(),
    )
    .unwrap();
    assert_eq!(cycle["seed"], base.derive_seed("econ-cycle").unwrap());

    // Both distinct pairs survive arbitration; every PROPOSED intent got
    // one intent_proposed row (py econ_cycle.py:170-182).
    let ordered = cycle["ordered"].as_array().unwrap();
    assert_eq!(ordered.len(), 2);
    let ledger = EconLedger::open(&ledger_path).unwrap();
    assert_eq!(ledger.count_events(Some("intent_proposed")).unwrap(), 2);
    let events = ledger.events(0).unwrap();
    let mut wire_ids: Vec<&str> = ordered
        .iter()
        .map(|i| i["intent_id"].as_str().unwrap())
        .collect();
    let mut ledger_ids: Vec<&str> = events.iter().map(|e| e.intent_id.as_str()).collect();
    wire_ids.sort_unstable();
    ledger_ids.sort_unstable();
    assert_eq!(wire_ids, ledger_ids);
    for event in &events {
        assert_eq!(event.cycle_id, cycle_id);
        assert_eq!(event.at, NOW);
        assert!(event.amounts.is_empty(), "py passes no amounts");
        assert_eq!(event.details["shadow_cycle"], true);
        // py econ_cycle.py:105 -- a rebalance intent's target is the
        // DEST channel id, verbatim.
        let target = event.details["target"].as_str().unwrap();
        assert!(
            target == "200x1x0" || target == "400x1x0",
            "target is the dest channel: {target}"
        );
        let explanation = event.details["explanation"].as_str().unwrap();
        assert!(
            explanation.starts_with("cycle_rebalance: ") && explanation.contains("score="),
            "py Explanation.render(): {explanation}"
        );
    }

    // Second call consumes the next sequence number (py `_econ_cycle_seq
    // += 1` on the module global).
    let again = econ_cycle_response(
        EconCycleSources {
            enabled: Ok(true),
            candidates: Some(Ok(vec![pair("100x1x0", "200x1x0", 50_000, 0.8)])),
            ledger_path: Some(ledger_path.clone()),
            now: NOW,
        },
        &seq,
    );
    assert_eq!(
        again["cycle"]["cycle_id"],
        format!("econ-cycle-{NOW}-2").as_str()
    );
}

/// py `ledger_for_reconciliation()` returning None (an open failure) skips
/// ledgering but the cycle still runs and returns (econ_cycle.py:168-169).
/// A directory is unopenable as a SQLite file.
#[test]
fn unopenable_ledger_still_returns_the_cycle_unledgered() {
    let dir = tempfile::tempdir().unwrap();
    let seq = AtomicI64::new(0);
    let v = econ_cycle_response(
        EconCycleSources {
            enabled: Ok(true),
            candidates: Some(Ok(vec![pair("100x1x0", "200x1x0", 50_000, 0.8)])),
            ledger_path: Some(dir.path().to_path_buf()),
            now: NOW,
        },
        &seq,
    );
    assert_eq!(v["enabled"], true);
    assert_eq!(v["cycle"]["intents_proposed"], 1);
}
