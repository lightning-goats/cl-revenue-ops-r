//! MODES table parity, pinned by `fixtures/rebalance/modes.json` (generated
//! from the REAL `modules/rebalance_modes.py` by
//! `tools/port/gen_rebalance_fixtures.py modes` in the port worktree,
//! branch `port`).
//!
//! `deadline_secs` is a Rust-only field (Python's `deadline` is a
//! documentation-only string class — see `revops_rebalance::modes`'s module
//! doc comment). This suite checks it via
//! `modes::deadline_secs_for_class(fixture_deadline_class)` — i.e. the
//! fixture pins the Python ground truth (the string class), and the test
//! confirms the Rust mapping table stays self-consistent with it, rather
//! than claiming a Python-sourced numeric value that doesn't exist.

use revops_rebalance::modes::{self, EngineKwargs, ModeRow};
use serde_json::Value;
use std::path::PathBuf;

const MODE_NAMES: [&str; 5] = [
    "normal",
    "hot_protection",
    "structural_drain",
    "manual",
    "diagnostic",
];

fn fixture() -> Value {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/rebalance/modes.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

#[test]
fn modes_table_matches_python_fixture() {
    let fx = fixture();
    let modes = fx["modes"].as_object().expect("modes object");
    assert_eq!(modes.len(), MODE_NAMES.len(), "exactly 5 modes");

    for name in MODE_NAMES {
        let row: &ModeRow = modes::mode(name).unwrap_or_else(|| panic!("mode {name} missing"));
        let fx_row = &modes[name];
        assert_eq!(fx_row["name"].as_str().unwrap(), name);
        assert_eq!(
            row.priority,
            fx_row["priority"].as_i64().unwrap(),
            "{name} priority"
        );
        assert_eq!(
            row.budget_bucket,
            fx_row["budget_bucket"].as_str().unwrap(),
            "{name} budget_bucket"
        );
        assert_eq!(
            row.reserve_on_rail,
            fx_row["reserve_on_rail"].as_bool().unwrap(),
            "{name} reserve_on_rail"
        );
        assert_eq!(
            row.account_costs,
            fx_row["account_costs"].as_bool().unwrap(),
            "{name} account_costs"
        );
        assert_eq!(
            row.accounting_owner,
            fx_row["accounting_owner"].as_str().unwrap(),
            "{name} accounting_owner"
        );
        // deadline_secs: Rust-only field, checked for self-consistency
        // against the Python ground-truth string class (see module doc).
        let class = fx_row["deadline"].as_str().unwrap();
        assert_eq!(
            row.deadline_secs,
            modes::deadline_secs_for_class(class),
            "{name} deadline_secs vs class {class:?}"
        );
    }
}

#[test]
fn engine_kwargs_matches_python_fixture_for_all_five_modes() {
    let fx = fixture();
    let fx_kwargs = fx["engine_kwargs"]
        .as_object()
        .expect("engine_kwargs object");
    assert_eq!(fx_kwargs.len(), MODE_NAMES.len());

    for name in MODE_NAMES {
        let kw: EngineKwargs = modes::engine_kwargs(name);
        let fx_kw = &fx_kwargs[name];
        assert_eq!(
            kw.reserve_budget,
            fx_kw["reserve_budget"].as_bool().unwrap(),
            "{name} reserve_budget"
        );
        assert_eq!(
            kw.account_costs,
            fx_kw["account_costs"].as_bool().unwrap(),
            "{name} account_costs"
        );
    }
}

/// Manual (priority 100) is the one mode that skips engine reservation and
/// accounting (P4-020): the frozen contract most later tasks care about.
#[test]
fn manual_mode_skips_reservation_per_fixture() {
    let fx = fixture();
    let manual = &fx["engine_kwargs"]["manual"];
    assert!(!manual["reserve_budget"].as_bool().unwrap());
    assert!(!manual["account_costs"].as_bool().unwrap());
}
