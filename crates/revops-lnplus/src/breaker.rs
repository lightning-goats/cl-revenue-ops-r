//! Circuit breaker — **defect #5**.
//!
//! **Incident.** The Python breaker (py 862-881 `breaker_tripped` /
//! `trip_breaker` / `clear_breaker`) trips BY DESIGN on every manually
//! -executed swap: `_reconcile` sees "LN+ shows opening swap N with no
//! local record" (py 989-995) and trips. That is correct — an untracked
//! commitment genuinely needs attention. The bug is what happens next:
//! once `revenue-lnplus-backfill` (or the automatic backfill choke point,
//! py 901-924) adopts the swap into the local ledger, the DIVERGENCE THAT
//! CAUSED THE TRIP IS GONE — but the breaker stays latched forever,
//! because Python's `_reconcile` only ever calls `trip_breaker` (which
//! B10-preserves the first reason and refuses to overwrite) and NEVER
//! calls `clear_breaker` itself; only an operator RPC does. Automation
//! stayed dead for 8 days until someone noticed and cleared it by hand.
//!
//! **Fix, and why it does not become a new incident.** [`BreakerCause`]
//! classifies every trip reason and marks exactly two variants
//! re-verifiable: [`BreakerCause::OpeningGhostNoLocalRecord`] and
//! [`BreakerCause::PendingGhostNoLocalRecord`] — the "no local record"
//! shape, and ONLY that shape, is resolved purely by backfill adopting the
//! row (a fact `reconcile.rs` can re-check on every pass with no new
//! external state). Every other cause variant's `is_reverifiable()` is
//! `false` and [`maybe_auto_clear`] refuses to clear them, full stop:
//! missed 48h deadlines, a local row missing/divergent from LN+'s live
//! listing, ambiguous funded-channel divergences, and LN+ outages all
//! require a human. This is the exact boundary the task spec draws.

use crate::ports::{LnPlusDb, LogLevel, Logger};

/// Python's `config_overrides` key (py 664 `_BREAKER_KEY`), kept here only
/// as documentation of the wire-compat key name a concrete `LnPlusDb`
/// implementation should use when persisting `get_breaker`/`set_breaker`.
pub const BREAKER_KEY: &str = "_lnplus_breaker";

/// Structured trip cause. Only the two `*GhostNoLocalRecord` variants are
/// ever auto-clearable — see [`BreakerCause::is_reverifiable`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BreakerCause {
    /// py 989-995. Re-verifiable: resolved once `swap_id` gets a local row
    /// (via backfill).
    OpeningGhostNoLocalRecord { swap_id: String },
    /// py 1004-1026 (excluding the B5b terminal-ghost cleanup path). Same
    /// re-verifiable shape as the opening-ghost case.
    PendingGhostNoLocalRecord { swap_id: String },
    /// py 984-987 (opening/opened rows absent from LN+'s live listing) and
    /// py 956-960 (local `applied` but LN+ already `completed`). NEVER
    /// auto-clear — a real divergence, not a bookkeeping gap.
    LocalRowDivergentFromRemote { swap_id: String, detail: String },
    /// py 1785-1793 `_maybe_trip_deadline_miss`. NEVER auto-clear.
    MissedOpenDeadline { swap_id: String },
    /// B7/I5(b)-class ambiguous funded-channel capacity match. NEVER
    /// auto-clear.
    AmbiguousFundedChannelDivergence { swap_id: String, detail: String },
    /// LN+ itself unreachable during a reconcile fetch (py 894-897 is a
    /// `return False` without tripping in Python — modeled here as a
    /// breaker cause variant for completeness/documentation of the "never
    /// auto-clear" contract; callers are not required to trip on outage
    /// alone, matching Python).
    LnPlusOutage { detail: String },
}

impl BreakerCause {
    /// The ONLY reverifiable shape: "LN+ lists a swap with no local row".
    /// Resolved purely by backfill; nothing else in this enum is.
    pub fn is_reverifiable(&self) -> bool {
        matches!(
            self,
            BreakerCause::OpeningGhostNoLocalRecord { .. }
                | BreakerCause::PendingGhostNoLocalRecord { .. }
        )
    }

    pub fn swap_id(&self) -> Option<&str> {
        match self {
            BreakerCause::OpeningGhostNoLocalRecord { swap_id }
            | BreakerCause::PendingGhostNoLocalRecord { swap_id }
            | BreakerCause::LocalRowDivergentFromRemote { swap_id, .. }
            | BreakerCause::MissedOpenDeadline { swap_id }
            | BreakerCause::AmbiguousFundedChannelDivergence { swap_id, .. } => Some(swap_id),
            BreakerCause::LnPlusOutage { .. } => None,
        }
    }

    pub fn message(&self) -> String {
        match self {
            BreakerCause::OpeningGhostNoLocalRecord { swap_id } => format!(
                "LN+ shows opening swap {swap_id} with no local record — if this swap was entered manually, run revenue-lnplus-backfill to adopt it"
            ),
            BreakerCause::PendingGhostNoLocalRecord { swap_id } => format!(
                "LN+ shows pending swap {swap_id} with no local record — if this swap was entered manually, run revenue-lnplus-backfill to adopt it"
            ),
            BreakerCause::LocalRowDivergentFromRemote { swap_id, detail } => {
                format!("local swap {swap_id} {detail}")
            }
            BreakerCause::MissedOpenDeadline { swap_id } => format!("missed 48h deadline for swap {swap_id}"),
            BreakerCause::AmbiguousFundedChannelDivergence { swap_id, detail } => {
                format!("swap {swap_id}: ambiguous funded-channel divergence — {detail}")
            }
            BreakerCause::LnPlusOutage { detail } => format!("LN+ outage during reconcile: {detail}"),
        }
    }
}

/// The breaker's persisted state (serialized into `config_overrides`'
/// `_lnplus_breaker` key by whatever the DB implementation chooses; this
/// crate only defines the in-process shape).
#[derive(Debug, Clone, PartialEq)]
pub struct BreakerState {
    pub tripped_at: i64,
    pub cause: BreakerCause,
}

/// py `trip_breaker` (865-877). B10: preserve the FIRST cause — a second
/// trip while already tripped changes nothing and is reported back to the
/// caller as `AlreadyTripped` purely for logging.
pub enum TripOutcome {
    NewTrip(BreakerState),
    AlreadyTripped(BreakerState),
}

pub fn trip(existing: Option<BreakerState>, cause: BreakerCause, now: i64) -> TripOutcome {
    match existing {
        Some(state) => TripOutcome::AlreadyTripped(state),
        None => TripOutcome::NewTrip(BreakerState {
            tripped_at: now,
            cause,
        }),
    }
}

/// The fix for defect #5: `true` iff `existing.cause` is reverifiable AND
/// the caller has determined (by re-checking the SAME condition that
/// produced the trip — see `reconcile.rs`) that it no longer reproduces.
/// Never inspects anything beyond that: a reverifiable cause whose
/// condition still holds stays latched (same reason as before, no new
/// trip needed thanks to B10), and a non-reverifiable cause NEVER clears
/// itself no matter what `still_reproducible` says.
pub fn maybe_auto_clear(existing: &BreakerState, still_reproducible: bool) -> bool {
    existing.cause.is_reverifiable() && !still_reproducible
}

/// DB-backed convenience wrapper over [`trip`]/[`maybe_auto_clear`] for
/// production wiring; `reconcile.rs`'s tests exercise the pure functions
/// directly.
pub fn tripped_message(db: &dyn LnPlusDb) -> Option<String> {
    db.get_breaker().map(|s| s.cause.message())
}

pub fn trip_and_persist(db: &dyn LnPlusDb, logger: &dyn Logger, cause: BreakerCause, now: i64) {
    match trip(db.get_breaker(), cause, now) {
        TripOutcome::NewTrip(state) => {
            let msg = state.cause.message();
            db.set_breaker(&state);
            logger.log(
                LogLevel::Error,
                &format!("LNPLUS: CIRCUIT BREAKER TRIPPED — {msg}"),
            );
        }
        TripOutcome::AlreadyTripped(existing) => {
            logger.log(
                LogLevel::Error,
                &format!(
                    "LNPLUS: CIRCUIT BREAKER TRIP — attempted trip suppressed (breaker already tripped: {})",
                    existing.cause.message()
                ),
            );
        }
    }
}

pub fn clear_and_persist(db: &dyn LnPlusDb, logger: &dyn Logger) {
    db.clear_breaker();
    logger.log(
        LogLevel::Info,
        "LNPLUS: circuit breaker cleared by operator",
    );
}

/// Auto-clear entry point used by `reconcile.rs`: clears the breaker
/// (logging distinctly from an operator-initiated clear) when the
/// CURRENTLY tripped cause is reverifiable and `still_reproducible` is
/// false. Returns `true` iff it actually cleared.
pub fn auto_clear_if_resolved(
    db: &dyn LnPlusDb,
    logger: &dyn Logger,
    still_reproducible: bool,
) -> bool {
    let Some(state) = db.get_breaker() else {
        return false;
    };
    if !maybe_auto_clear(&state, still_reproducible) {
        return false;
    }
    db.clear_breaker();
    logger.log(
        LogLevel::Info,
        &format!(
            "LNPLUS: circuit breaker auto-cleared — reverifiable cause resolved ({})",
            state.cause.message()
        ),
    );
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reverifiable_causes_are_exactly_the_two_ghost_variants() {
        assert!(BreakerCause::OpeningGhostNoLocalRecord {
            swap_id: "1".into()
        }
        .is_reverifiable());
        assert!(BreakerCause::PendingGhostNoLocalRecord {
            swap_id: "1".into()
        }
        .is_reverifiable());
    }

    #[test]
    fn never_auto_clear_classes_are_not_reverifiable() {
        assert!(!BreakerCause::MissedOpenDeadline {
            swap_id: "1".into()
        }
        .is_reverifiable());
        assert!(!BreakerCause::LocalRowDivergentFromRemote {
            swap_id: "1".into(),
            detail: "x".into()
        }
        .is_reverifiable());
        assert!(!BreakerCause::AmbiguousFundedChannelDivergence {
            swap_id: "1".into(),
            detail: "x".into()
        }
        .is_reverifiable());
        assert!(!BreakerCause::LnPlusOutage { detail: "x".into() }.is_reverifiable());
    }

    // -- maybe_auto_clear: the 8-day-incident fix, proven --

    #[test]
    fn ghost_cause_clears_once_backfill_resolves_it() {
        let state = BreakerState {
            tripped_at: 100,
            cause: BreakerCause::OpeningGhostNoLocalRecord {
                swap_id: "42".into(),
            },
        };
        // The condition ("no local record") no longer reproduces.
        assert!(maybe_auto_clear(&state, false));
    }

    #[test]
    fn control_ghost_cause_stays_latched_while_condition_still_holds() {
        // CONTROL: same reverifiable cause, but the trigger condition is
        // STILL true (backfill has not run yet / failed) -- must NOT
        // clear. Proves the assertion above is not vacuously true.
        let state = BreakerState {
            tripped_at: 100,
            cause: BreakerCause::OpeningGhostNoLocalRecord {
                swap_id: "42".into(),
            },
        };
        assert!(!maybe_auto_clear(&state, true));
    }

    #[test]
    fn missed_deadline_never_auto_clears_even_if_reproducible_is_false() {
        // CONTROL for the "NEVER auto-clear" classes: even telling the
        // function the condition no longer reproduces must not clear a
        // non-reverifiable cause. If someone widened `is_reverifiable`
        // to cover this variant, this assertion goes from pass to fail.
        let state = BreakerState {
            tripped_at: 100,
            cause: BreakerCause::MissedOpenDeadline {
                swap_id: "42".into(),
            },
        };
        assert!(!maybe_auto_clear(&state, false));
    }

    #[test]
    fn local_row_divergence_never_auto_clears() {
        let state = BreakerState {
            tripped_at: 100,
            cause: BreakerCause::LocalRowDivergentFromRemote {
                swap_id: "42".into(),
                detail: "still applied but LN+ completed".into(),
            },
        };
        assert!(!maybe_auto_clear(&state, false));
    }

    #[test]
    fn ambiguous_funded_channel_divergence_never_auto_clears() {
        let state = BreakerState {
            tripped_at: 100,
            cause: BreakerCause::AmbiguousFundedChannelDivergence {
                swap_id: "42".into(),
                detail: "dual-fund capacity mismatch".into(),
            },
        };
        assert!(!maybe_auto_clear(&state, false));
    }

    #[test]
    fn lnplus_outage_never_auto_clears() {
        let state = BreakerState {
            tripped_at: 100,
            cause: BreakerCause::LnPlusOutage {
                detail: "timeout".into(),
            },
        };
        assert!(!maybe_auto_clear(&state, false));
    }

    // -- trip: B10 preserve-first-cause --

    #[test]
    fn first_trip_is_a_new_trip() {
        let outcome = trip(
            None,
            BreakerCause::MissedOpenDeadline {
                swap_id: "1".into(),
            },
            100,
        );
        assert!(matches!(outcome, TripOutcome::NewTrip(_)));
    }

    #[test]
    fn second_trip_preserves_first_cause_b10() {
        let first = BreakerState {
            tripped_at: 100,
            cause: BreakerCause::MissedOpenDeadline {
                swap_id: "1".into(),
            },
        };
        let outcome = trip(
            Some(first.clone()),
            BreakerCause::OpeningGhostNoLocalRecord {
                swap_id: "2".into(),
            },
            200,
        );
        match outcome {
            TripOutcome::AlreadyTripped(state) => assert_eq!(state, first),
            TripOutcome::NewTrip(_) => panic!("must preserve first cause"),
        }
    }
}
