//! `ExecutionMode` — the wiring layer's own kill switch, independent of
//! [`crate::config::LnPlusConfig`]'s `lnplus_execute_applications` /
//! `planner_dry_run` fields.
//!
//! ABSOLUTE SAFETY REQUIREMENT (task spec, LN+ wiring layer): execution is
//! OFF by default. Every effect that "applies to a swap, withdraws an
//! application, or opens a channel" — the mutating half of
//! [`crate::ports::LnPlusApi`] (`create_application` / `delete_application`
//! / `complete_application` / `create_rating` / `mark_read_notifications`)
//! and the money-moving half of [`crate::ports::ChainPort`] (`connect` /
//! `fund_channel`) — must go through [`crate::gated::GatedLnPlusApi`] /
//! [`crate::gated::GatedChainPort`], both of which are built from an
//! explicit `ExecutionMode`.
//!
//! `cfg.lnplus_execute_applications` already exists in the ported kernel
//! (`evaluator.rs`'s `select_and_apply`, py 636) and independently gates
//! whether `run_cycle` even ATTEMPTS an application. `ExecutionMode` is a
//! second, wiring-layer-owned gate that sits in front of the port itself:
//! even if a config value is mis-set to "execute", a caller who has not
//! also explicitly constructed `ExecutionMode::Armed` cannot reach a live
//! `create_application`/`connect`/`fund_channel` call. The two gates are
//! deliberately independent — belt and suspenders, not one relabeled as
//! two.
//!
//! `ExecutionMode::default()` is [`ExecutionMode::DryRun`]: any caller who
//! builds one via `Default::default()`, `..Default::default()` struct
//! update syntax, or simply forgets to set it explicitly, gets the SAFE
//! behavior. There is no way to construct an armed mode by omission — the
//! only way to reach [`ExecutionMode::Armed`] is to name it.

/// See the module doc comment. `DryRun` is the [`Default`] — the ONLY
/// variant reachable by a caller who does not explicitly opt in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExecutionMode {
    /// Every mutating [`crate::ports::LnPlusApi`] / [`crate::ports::ChainPort`]
    /// call is suppressed (logged, then an `Err` returned) by
    /// [`crate::gated::GatedLnPlusApi`] / [`crate::gated::GatedChainPort`].
    /// Read-only calls on both traits are unaffected in every mode.
    #[default]
    DryRun,
    /// Mutating calls reach the real, injected port unmodified.
    Armed,
}

impl ExecutionMode {
    /// `true` iff live, money-moving / commitment-making calls are allowed
    /// through. The single predicate every gate in [`crate::gated`] checks.
    pub fn is_armed(self) -> bool {
        matches!(self, ExecutionMode::Armed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_dry_run_not_armed() {
        // The control this whole module exists to satisfy: a caller who
        // never explicitly names `Armed` gets the safe variant, whether via
        // `ExecutionMode::default()` or `..Default::default()`.
        assert_eq!(ExecutionMode::default(), ExecutionMode::DryRun);
        assert!(!ExecutionMode::default().is_armed());
    }

    #[test]
    fn armed_is_the_only_way_to_get_true() {
        assert!(ExecutionMode::Armed.is_armed());
        assert!(!ExecutionMode::DryRun.is_armed());
    }
}
