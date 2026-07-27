//! Gate 14: finalize / rate / release. Ports `_finalize` (py 1868-1937) —
//! **defect #1** — `_try_create_rating` (1940-1961) — **defect #2** — and
//! `_retry_pending_ratings` (1963-1983).
//!
//! ## Defect #1: the caller must consume finalize's result
//!
//! **Incident.** `_finalize` (py 1890-1902) correctly returns EARLY —
//! `None`, leaving the row `"active"`, no rating filed — when
//! `listpeerchannels` fails: the still-open heuristic is meaningless
//! without a genuine RPC answer, so filing a permanent negative rating on
//! a transient hiccup would be much worse than retrying next pass (B2).
//! But the caller, `run_watcher_once`'s phase 5 loop (py 1455-1461),
//! does:
//! ```python
//! self._finalize(row)
//! summary["finalized"].append(sid)
//! ```
//! unconditionally — `_finalize`'s return value (`None`) is discarded, so
//! the pass reports the swap as finalized EVERY TIME phase 5 reaches it,
//! whether or not anything actually happened.
//!
//! **Fix.** [`finalize`] returns [`FinalizeOutcome`], an enum the caller
//! cannot silently discard the meaning of: only [`FinalizeOutcome::Finalized`]
//! represents a real terminal transition; [`FinalizeOutcome::Deferred`]
//! makes the "no state change happened" case a distinct value instead of
//! Python's `None` that a `.append()` call ignored. `tests/finalize.rs`
//! drives a watcher-phase-5-shaped caller against both variants and
//! proves only `Finalized` may be counted.
//!
//! ## Defect #2: rating idempotency must be structural, not a blob scan
//!
//! **Incident.** `_try_create_rating` (py 1946-1949):
//! ```python
//! blob = str(errors or e).lower()
//! if "already" in blob or "once" in blob:
//!     return True   # treated as filed
//! ```
//! `blob` is either the parsed `errors` dict OR THE WHOLE EXCEPTION
//! stringified — a substring scan over the entire raw response body (or,
//! worse, whatever free text is in the exception's `message`). ANY error
//! response containing "already" or "once" ANYWHERE — a rate-limit
//! message ("try again once per minute"), an unrelated field's validation
//! error, a connection-layer wrapper string — permanently marks the
//! rating as filed, silently dropping a rating that should have been
//! retried.
//!
//! **Fix.** [`rating_already_filed`] is the exact same STRUCTURAL
//! primitive `open.rs::already_past_opening` and `withdrawal.rs`'s
//! classifier already use: `http_status == 422` AND the phrase appears in
//! one of the PARSED `errors` dict's per-field messages — never the raw
//! body, never the exception's free-text `message`.

use crate::activate::{incoming_channel_open, release_no_close_if_ours, TagColumn};
use crate::db_types::{SwapPatch, SwapRow};
use crate::error::LnPlusError;
use crate::ports::{ChainPort, IgnorePeerPort, LnPlusApi, LnPlusDb, LogLevel, Logger, PolicyPort};
use crate::types::Rating;

/// py 1938 `_RATING_RETRY_WINDOW_SECONDS`.
pub const RATING_RETRY_WINDOW_SECONDS: i64 = 7 * 86_400;

/// The fix for defect #2: structural http_status+errors match.
pub fn rating_already_filed(e: &LnPlusError) -> bool {
    e.structural_contains(422, "already") || e.structural_contains(422, "once")
}

/// py `_try_create_rating` (1940-1961). `true` = filed (or LN+ says it
/// already was); `false` = transient failure worth retrying next pass.
pub fn try_create_rating(
    sid: &str,
    rating: Rating,
    api: &dyn LnPlusApi,
    logger: &dyn Logger,
) -> bool {
    match api.create_rating(sid, rating) {
        Ok(()) => true,
        Err(e) => {
            if rating_already_filed(&e) {
                logger.log(LogLevel::Info, &format!("LNPLUS: create_rating for swap {sid}: LN+ reports already rated — treating as filed"));
                true
            } else {
                logger.log(
                    LogLevel::Warn,
                    &format!("LNPLUS: create_rating failed for swap {sid}: {e}"),
                );
                false
            }
        }
    }
}

/// The fix for defect #1: a caller cannot mistake "nothing happened" for
/// "finalized" without explicitly matching this enum.
#[derive(Debug, Clone, PartialEq)]
pub enum FinalizeOutcome {
    /// The row reached a genuine terminal status this call.
    Finalized { status: String, outcome: String },
    /// B2: no state change — `listpeerchannels` failed and the still-open
    /// heuristic could not be evaluated. Retry next pass. Callers MUST
    /// NOT count this as a finalized swap.
    Deferred { reason: String },
}

impl FinalizeOutcome {
    pub fn is_finalized(&self) -> bool {
        matches!(self, FinalizeOutcome::Finalized { .. })
    }
}

/// py `_finalize` (1868-1937).
pub fn finalize(
    row: &SwapRow,
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    policy: &dyn PolicyPort,
    ignore_peer: Option<&dyn IgnorePeerPort>,
    chain: &dyn ChainPort,
    logger: &dyn Logger,
) -> FinalizeOutcome {
    let sid = row.swap_id.clone();
    let outbound_peer = row.outbound_peer.clone();

    let Some(incoming_peer) = row.incoming_peer.clone().filter(|p| !p.is_empty()) else {
        // C-8: cannot judge a counterparty we cannot even identify.
        release_no_close_if_ours(
            &sid,
            row,
            outbound_peer.as_deref(),
            TagColumn::Outbound,
            db,
            policy,
            logger,
        );
        release_no_close_if_ours(
            &sid,
            row,
            row.incoming_peer.as_deref(),
            TagColumn::Incoming,
            db,
            policy,
            logger,
        );
        db.update_swap(
            &sid,
            &SwapPatch::default()
                .status("ended")
                .outcome("ended_unjudged"),
        );
        logger.log(
            LogLevel::Warn,
            &format!("LNPLUS: finalize for swap {sid} — cannot judge counterparty (incoming_peer is NULL/empty); ending unjudged, no rating filed"),
        );
        return FinalizeOutcome::Finalized {
            status: "ended".to_string(),
            outcome: "ended_unjudged".to_string(),
        };
    };

    let Some(positive) = incoming_channel_open(Some(&incoming_peer), chain) else {
        // B2: RPC failure, not an authoritative "no channel" -- no state
        // change, retry next pass.
        logger.log(
            LogLevel::Warn,
            &format!("LNPLUS: finalize for swap {sid} deferred — listpeerchannels failed (unknown channel state), not defaulting to a negative rating; will retry next pass"),
        );
        return FinalizeOutcome::Deferred {
            reason: "listpeerchannels failed".to_string(),
        };
    };
    let rating = if positive {
        Rating::Positive
    } else {
        Rating::Negative
    };

    let rating_filed = try_create_rating(&sid, rating, api, logger);

    release_no_close_if_ours(
        &sid,
        row,
        outbound_peer.as_deref(),
        TagColumn::Outbound,
        db,
        policy,
        logger,
    );
    release_no_close_if_ours(
        &sid,
        row,
        Some(incoming_peer.as_str()),
        TagColumn::Incoming,
        db,
        policy,
        logger,
    );

    db.bump_peer(&incoming_peer, !positive, Some(rating));
    if let Some(outbound_peer) = &outbound_peer {
        db.bump_peer(outbound_peer, false, None);
    }

    if !positive {
        if let Some(ignore_fn) = ignore_peer {
            if let Err(e) = ignore_fn.ignore_peer(&incoming_peer, "LN+ swap defection") {
                logger.log(
                    LogLevel::Warn,
                    &format!("LNPLUS: ignore_peer_fn failed for {incoming_peer}: {e}"),
                );
            }
        }
    }

    let final_status = if rating_filed {
        "ended"
    } else {
        "ended_rating_pending"
    };
    db.update_swap(
        &sid,
        &SwapPatch::default()
            .status(final_status)
            .outcome(rating.as_str()),
    );
    FinalizeOutcome::Finalized {
        status: final_status.to_string(),
        outcome: rating.as_str().to_string(),
    }
}

/// py `_retry_pending_ratings` (1963-1983). Bounded: gives up (-> `ended`)
/// once more than [`RATING_RETRY_WINDOW_SECONDS`] past the contract end.
pub fn retry_pending_ratings(
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    logger: &dyn Logger,
    now: i64,
) {
    for row in db.get_swaps_by_status(&["ended_rating_pending"]) {
        let sid = row.swap_id.clone();
        let rating = if row.outcome.as_deref() == Some("negative") {
            Rating::Negative
        } else {
            Rating::Positive
        };
        let deadline = row.ends_at.unwrap_or(now) + RATING_RETRY_WINDOW_SECONDS;
        if try_create_rating(&sid, rating, api, logger) {
            db.update_swap(&sid, &SwapPatch::default().status("ended"));
        } else if now > deadline {
            logger.log(LogLevel::Warn, &format!("LNPLUS: giving up on rating swap {sid} after 7 days of retries — ending unrated"));
            db.update_swap(
                &sid,
                &SwapPatch::default()
                    .status("ended")
                    .outcome(format!("{}_rating_unfiled", rating.as_str())),
            );
        }
    }
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
    fn rating_already_filed_structural_match() {
        let e = LnPlusError::with_errors(
            "boom",
            422,
            errors_map("rating", "you may only rate once per swap"),
        );
        assert!(rating_already_filed(&e));
        let e2 = LnPlusError::with_errors(
            "boom",
            422,
            errors_map("rating", "this swap was already rated"),
        );
        assert!(rating_already_filed(&e2));
    }

    #[test]
    fn control_wrong_status_is_not_treated_as_filed() {
        // CONTROL: same message text, wrong status -- must not match. A
        // 500 saying "already rated" in its body is a server error, not a
        // confirmed idempotent duplicate.
        let e = LnPlusError::with_errors("boom", 500, errors_map("rating", "already rated"));
        assert!(!rating_already_filed(&e));
    }

    #[test]
    fn control_unrelated_free_text_message_is_not_treated_as_filed_defect_2() {
        // CONTROL reproducing the exact defect #2 shape: the free-text
        // `message` contains "already"/"once" but there is no parsed
        // `errors` dict backing it up (e.g. an unstructured response).
        // Python's `str(errors or e).lower()` would have matched this;
        // the fix must not.
        let e = LnPlusError::with_status(
            "rate limited: you may only call this endpoint once per minute",
            429,
        );
        assert!(!rating_already_filed(&e));
        let e2 = LnPlusError::with_status("LN+ says we already have an open connection", 422);
        assert!(
            !rating_already_filed(&e2),
            "422 with a matching word in free-text `message` but no parsed errors dict must not match"
        );
    }
}
