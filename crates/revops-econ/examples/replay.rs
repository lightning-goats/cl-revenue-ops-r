//! Replay an econ_ledger.db and print the resulting LedgerState as
//! canonical JSON — the cross-language parity instrument for comparing
//! against Python's `EconLedger.replay()` on the same database file.
//!
//! Usage: cargo run -p revops-econ --example replay -- /path/to/econ_ledger.db

use revops_core::canonical::canonical_json;
use revops_econ::ledger::EconLedger;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: replay <econ_ledger.db>")?;
    let ledger = EconLedger::open(&path)?;
    let state = ledger.replay()?;

    let reserved: serde_json::Map<String, serde_json::Value> = state
        .reserved_msat
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::json!(v)))
        .collect();
    let spent: serde_json::Map<String, serde_json::Value> = state
        .spent_msat
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::json!(v)))
        .collect();
    let terminal: serde_json::Map<String, serde_json::Value> = state
        .terminal
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::json!(v)))
        .collect();

    let out = serde_json::json!({
        "anomalies": state.anomalies,
        "reserved_msat": reserved,
        "spent_msat": spent,
        "terminal": terminal,
        "total_spent_msat": state.total_spent_msat,
    });
    println!("{}", canonical_json(&out)?);
    Ok(())
}
