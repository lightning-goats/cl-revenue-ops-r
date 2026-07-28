//! Pending-application timeout — **defect #3**.
//!
//! **Incident.** py `_handle_pending_timeouts` (2067-2086) calls
//! `delete_application(sid)` and, on ANY exception, logs a warning and
//! `continue`s — leaving the row at `applied` for the next hourly pass to
//! retry. But `delete_application` can fail with a documented, TERMINAL
//! 422 response: "no application" (there is nothing left to delete — we
//! already withdrew, or LN+ already removed it) or "not pending" (the
//! swap advanced past pending — filled and is now opening/completed).
//! Neither of those is a transient failure worth retrying. Retrying them
//! forever pins the row at `applied`, which is one of the
//! [`crate::db_types::INFLIGHT_STATUSES`] — so `has_inflight()` stays
//! `true` and the one-application-per-node serialization gate blocks new
//! applications indefinitely on a swap that, per LN+, is already resolved.
//! That is the "false inflight state" the task spec names.
//!
//! **Fix.** [`classify_delete_application_error`] does a STRUCTURAL
//! http_status+errors match (same primitive as defect #2's
//! `rating_already_filed`) and returns a three-way
//! [`DeleteApplicationOutcome`] instead of Python's uniform
//! log-and-retry: `Withdrawn` (already gone — same as a clean success),
//! `Advanced` (swap progressed — stop retrying delete, but do NOT mark
//! withdrawn; the normal opening/completed flow in `open.rs`/`activate.rs`
//! now owns the row), or `Retryable` (genuinely transient — same
//! log-and-retry-next-pass Python already does).

use crate::db_types::SwapPatch;
use crate::error::LnPlusError;
use crate::ports::{LnPlusApi, LnPlusDb, LogLevel, Logger, PortResult};
use crate::types::MySwaps;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteApplicationOutcome {
    /// Success, or LN+'s 422 says there is no application left to delete
    /// (we already withdrew / it never existed on LN+'s side). Safe to
    /// mark the local row `withdrawn`.
    Withdrawn,
    /// LN+'s 422 says the swap is no longer pending — it advanced (filled
    /// -> opening/completed) while our withdrawal request was in flight.
    /// Must NOT mark `withdrawn` (that would be a lie); must NOT keep
    /// retrying `delete_application` either (there is nothing left to
    /// delete). The row stays `applied` and the normal reconcile/opening
    /// machinery picks it up next pass.
    Advanced,
    /// Anything else: a genuinely transient failure. Retry next pass,
    /// exactly like Python's uniform behaviour.
    Retryable,
}

/// The fix: structural http_status+errors classification, mirroring the
/// ALREADY-CORRECT pattern this same file uses for `complete_application`
/// (`open.rs`'s `already_past_opening`, py 1701-1717) instead of inventing
/// a new one.
pub fn classify_delete_application_error(err: &LnPlusError) -> DeleteApplicationOutcome {
    if err.structural_contains(422, "no application") {
        return DeleteApplicationOutcome::Withdrawn;
    }
    if err.structural_contains(422, "not pending") {
        return DeleteApplicationOutcome::Advanced;
    }
    DeleteApplicationOutcome::Retryable
}

/// py `_handle_pending_timeouts` (2067-2086), phase 6 of the watcher.
///
/// Task 61 4A: the withdrawn transition is a CAS from `"applied"` and a
/// persistence failure aborts with `Err` — a swap is only reported
/// withdrawn when its terminal write actually landed.
pub fn handle_pending_timeouts(
    my: &MySwaps,
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    logger: &dyn Logger,
    timeout_days: i64,
    now: i64,
) -> PortResult<Vec<String>> {
    let mut withdrawn = Vec::new();
    let cutoff = now - timeout_days * 86_400;
    let pending_ids = my.pending_ids();

    for row in db.get_swaps_by_status(&["applied"]) {
        let sid = row.swap_id.clone();
        if row.applied_at > cutoff {
            continue;
        }
        if !pending_ids.contains(&sid) {
            continue;
        }
        match api.delete_application(&sid) {
            Ok(()) => {
                if db.cas_swap(
                    &sid,
                    &["applied"],
                    &SwapPatch::default().status("withdrawn"),
                )? == crate::ports::CasOutcome::Applied
                {
                    withdrawn.push(sid);
                }
            }
            Err(e) => match classify_delete_application_error(&e) {
                DeleteApplicationOutcome::Withdrawn => {
                    logger.log(
                        LogLevel::Info,
                        &format!("LNPLUS: delete_application for {sid}: LN+ reports no application left — treating as withdrawn"),
                    );
                    if db.cas_swap(
                        &sid,
                        &["applied"],
                        &SwapPatch::default().status("withdrawn"),
                    )? == crate::ports::CasOutcome::Applied
                    {
                        withdrawn.push(sid);
                    }
                }
                DeleteApplicationOutcome::Advanced => {
                    logger.log(
                        LogLevel::Info,
                        &format!("LNPLUS: delete_application for {sid}: LN+ reports the swap is no longer pending — it advanced; not marking withdrawn, not retrying delete"),
                    );
                }
                DeleteApplicationOutcome::Retryable => {
                    logger.log(
                        LogLevel::Warn,
                        &format!("LNPLUS: delete_application failed for {sid}: {e}"),
                    );
                }
            },
        }
    }
    Ok(withdrawn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorsMap;

    fn errors_map(field: &str, msg: &str) -> ErrorsMap {
        let mut m = ErrorsMap::new();
        m.insert(field.to_string(), vec![msg.to_string()]);
        m
    }

    #[test]
    fn no_application_response_classified_as_withdrawn() {
        let e = LnPlusError::with_errors("boom", 422, errors_map("id", "no application found"));
        assert_eq!(
            classify_delete_application_error(&e),
            DeleteApplicationOutcome::Withdrawn
        );
    }

    #[test]
    fn not_pending_response_classified_as_advanced() {
        let e = LnPlusError::with_errors("boom", 422, errors_map("id", "swap is not pending"));
        assert_eq!(
            classify_delete_application_error(&e),
            DeleteApplicationOutcome::Advanced
        );
    }

    #[test]
    fn control_generic_422_without_terminal_phrase_is_retryable() {
        // CONTROL: a 422 that is neither documented terminal shape must
        // still retry -- proves the match is on the specific phrases, not
        // "any 422 is terminal".
        let e = LnPlusError::with_errors("boom", 422, errors_map("id", "malformed id"));
        assert_eq!(
            classify_delete_application_error(&e),
            DeleteApplicationOutcome::Retryable
        );
    }

    #[test]
    fn control_connection_error_is_retryable_even_with_terminal_words() {
        // CONTROL for the defect itself: an unstructured connection-level
        // error whose free text happens to contain "no application" must
        // NOT be classified terminal -- only a genuine 422 + parsed
        // errors dict counts. This is exactly the shape of input that
        // would have been misclassified by Python's uniform "any
        // exception -> retry" AND by a naive whole-blob substring scan in
        // the other direction.
        let e = LnPlusError::new("connect timeout: no application server responding");
        assert_eq!(
            classify_delete_application_error(&e),
            DeleteApplicationOutcome::Retryable
        );
    }
}
