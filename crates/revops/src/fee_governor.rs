//! `GovernorWiring`: owns the Rust-side governor objects for the plugin
//! lifetime (Phase 4b Task 5, checklist item 6).
//!
//! During the dry-run window the ledger is the Rust plugin's OWN file
//! (`<journal_dir>/econ_ledger_dryrun.db`) — NEVER the production
//! `econ_ledger.db`: governed authorization only APPENDS
//! `intent_proposed`/`intent_authorized` events for offline inspection,
//! and Python continues to own the production ledger until cutover
//! (Global Constraint: Python stays authoritative for the whole window;
//! any new write target must be a Rust-owned file next to
//! `revops-r-observer.db`).
//!
//! No fee-broadcast RPC call (or any other broadcast) exists anywhere in
//! this module or the crate it wires into — that is cutover work. (The
//! broadcast RPC's name is deliberately not spelled out here:
//! `tests/fee_scheduler.rs`' source-scan guard asserts the literal is
//! absent from this whole crate.)

use std::path::Path;

use revops_econ::arbiter::ActiveIntentRegistry;
use revops_econ::ledger::EconLedger;
use revops_fees::cycle::FeeCfgSnapshot;
use revops_fees::execution::GovernedDeps;

/// Filename of the dry-run-owned ledger, resolved under the plugin's
/// journal directory — never the production ledger's name or location.
const LEDGER_DRYRUN_FILENAME: &str = "econ_ledger_dryrun.db";

/// Owns the Rust-side governor objects for the plugin lifetime.
pub struct GovernorWiring {
    ledger: Option<EconLedger>,
    registry: ActiveIntentRegistry,
}

impl GovernorWiring {
    /// Opens the dry-run ledger at `<journal_dir>/econ_ledger_dryrun.db`.
    ///
    /// `journal_dir: None` (dry-run journaling disabled) or an open
    /// failure both yield `ledger: None`, logged once to stderr, rather
    /// than panicking — an unavailable ledger degrades the dry-run
    /// governor trail, it must never take down the plugin. The registry
    /// is always constructed (legacy rules only; no extended-rules
    /// provider wired in this phase).
    pub fn open(journal_dir: Option<&Path>) -> Self {
        let ledger = journal_dir.and_then(|dir| {
            let path = dir.join(LEDGER_DRYRUN_FILENAME);
            match EconLedger::open(&path) {
                Ok(ledger) => Some(ledger),
                Err(e) => {
                    eprintln!(
                        "revops: failed to open dry-run econ ledger at {}: {e}",
                        path.display()
                    );
                    None
                }
            }
        });
        GovernorWiring {
            ledger,
            registry: ActiveIntentRegistry::new(None),
        }
    }

    /// Borrow as [`GovernedDeps`] for one cycle. `paused`/`authority_level`
    /// come from the CURRENT cycle's [`FeeCfgSnapshot`] (T1) — never
    /// cached at [`open`](Self::open) time — so a runtime
    /// `revenue-config set` change is visible starting the very next
    /// cycle. Returned unconditionally; the caller (the cycle) gates on
    /// `cfg.econ_governor_fees_enabled` per `CycleDeps::governed`'s own
    /// doc contract.
    pub fn governed_deps<'a>(&'a self, cfg: &FeeCfgSnapshot) -> GovernedDeps<'a> {
        GovernedDeps {
            ledger: self.ledger.as_ref(),
            registry: Some(&self.registry),
            paused: cfg.paused,
            authority_level: cfg.authority_level.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with(paused: bool, authority_level: Option<&str>) -> FeeCfgSnapshot {
        FeeCfgSnapshot {
            paused,
            authority_level: authority_level.map(str::to_string),
            ..FeeCfgSnapshot::default()
        }
    }

    #[test]
    fn governed_deps_mirrors_cfg_paused_and_authority() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let wiring = GovernorWiring::open(Some(dir.path()));

        let cfg = cfg_with(true, Some("observe"));
        let deps = wiring.governed_deps(&cfg);
        assert!(deps.paused);
        assert_eq!(deps.authority_level.as_deref(), Some("observe"));

        let cfg2 = cfg_with(false, Some("capital"));
        let deps2 = wiring.governed_deps(&cfg2);
        assert!(!deps2.paused);
        assert_eq!(deps2.authority_level.as_deref(), Some("capital"));
    }

    #[test]
    fn open_without_dir_yields_none_ledger_and_still_constructs() {
        let wiring = GovernorWiring::open(None);
        let cfg = FeeCfgSnapshot::default();
        let deps = wiring.governed_deps(&cfg);
        assert!(deps.ledger.is_none());
        // Still constructs a usable registry even with no ledger.
        assert!(deps.registry.is_some());
    }

    #[test]
    fn ledger_path_is_dryrun_file_not_production() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let wiring = GovernorWiring::open(Some(dir.path()));
        let cfg = FeeCfgSnapshot::default();
        let deps = wiring.governed_deps(&cfg);
        assert!(deps.ledger.is_some(), "ledger should have opened");

        let expected_path = dir.path().join("econ_ledger_dryrun.db");
        assert!(
            expected_path.exists(),
            "expected dry-run ledger file at {}",
            expected_path.display()
        );
        assert!(expected_path.ends_with("econ_ledger_dryrun.db"));
        assert_ne!(
            expected_path.file_name().and_then(|n| n.to_str()),
            Some("econ_ledger.db"),
            "must never be the production ledger filename"
        );
    }
}
