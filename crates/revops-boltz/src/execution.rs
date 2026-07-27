//! Execution-mode gate for every wiring-layer entry point that can spend
//! real funds (create/claim/refund/withdraw).
//!
//! Per the wiring-layer task brief's ABSOLUTE SAFETY REQUIREMENTS:
//! "Execution must be OFF by default. Any function that could
//! create/claim/refund/withdraw a swap must require an explicit enable flag
//! AND a dry-run default. Model it as a type ... where `DryRun` is
//! `Default`. A caller must not be able to spend by forgetting a
//! parameter."
//!
//! This is stricter than py's `dry_run: bool = False` parameter
//! (`autocycle::DRY_RUN_DEFAULT` documents that the *auto-cycle* daemon/RPC
//! defaults to live execution, per operator ruling DD4/P1-018). This type
//! is a DIFFERENT, crate-wide mechanical guard sitting one layer further
//! out: every `commands::execute_*`/`driver::run_balance_cycle_pass`
//! function in this crate takes an [`ExecutionMode`] and its `Default` is
//! [`ExecutionMode::DryRun`] — a caller who constructs one with
//! `..Default::default()`, forgets the argument in a builder, or passes
//! `ExecutionMode::default()` gets the safe (non-spending) behaviour, never
//! the dangerous one. Only spelling `ExecutionMode::Armed` out loud arms a
//! spend path. `autocycle::DRY_RUN_DEFAULT` remains the source of truth for
//! what a *live* daemon loop's own `dry_run` config default should be; a
//! live adapter is expected to map `!dry_run` (config) to
//! [`ExecutionMode::Armed`] explicitly at the boundary, not silently.

/// Whether a wiring-layer action is allowed to perform a real,
/// fund-moving `boltzcli` subprocess call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExecutionMode {
    /// Safe default: no subprocess call that could create/claim/refund/
    /// withdraw is made. Callers still get the argv that WOULD have been
    /// run (see `commands::ActionOutcome::Preview`) so a preview is
    /// inspectable without ever touching a real `boltzcli`.
    #[default]
    DryRun,
    /// Explicitly armed: the caller has opted into performing the real
    /// subprocess call. There is no other way to reach this state — it is
    /// never the default and never reachable via `..Default::default()`.
    Armed,
}

impl ExecutionMode {
    /// `true` only for [`ExecutionMode::Armed`].
    pub fn is_armed(self) -> bool {
        matches!(self, ExecutionMode::Armed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_dry_run() {
        assert_eq!(ExecutionMode::default(), ExecutionMode::DryRun);
    }

    #[test]
    fn only_armed_reports_is_armed() {
        assert!(!ExecutionMode::DryRun.is_armed());
        assert!(ExecutionMode::Armed.is_armed());
    }

    #[test]
    fn default_constructed_value_is_never_armed() {
        // Control against the exact failure mode named in the brief: a
        // caller who builds an ExecutionMode via `..Default::default()`
        // (e.g. inside a larger config struct) must land on DryRun, not
        // silently on Armed.
        let mode: ExecutionMode = Default::default();
        assert!(!mode.is_armed());
    }
}
