//! Ledger-wide aggregates read by the capacity planner and operator
//! status RPCs — **defect #4** lives here.
//!
//! **Incident.** `database.py`'s `lnplus_reserved_sats()` (7619-7629) sums
//! `capacity_sats` for every row in `_LNPLUS_INFLIGHT_STATUSES =
//! ("applied", "opening", "opened")` whose `channel_funding_txid` is still
//! NULL, and the capacity planner subtracts that sum from the wallet's
//! spendable balance (`capacity_planner.py:558-562, 2354-2361`) so it
//! never double-commits funds a swap has already claimed.
//!
//! `_maybe_trip_deadline_miss` (py 1785-1793) is the one place a row can
//! become permanently un-actionable — the breaker trips and stays
//! latched (by design; see `breaker.rs`) — but it NEVER changes the row's
//! `status` column. The row is left at `"opening"`, which is still in
//! `_LNPLUS_INFLIGHT_STATUSES`. So a swap whose 48h deadline was missed
//! keeps consuming its full `capacity_sats` against the planner's
//! available-funds calculation forever, and keeps showing up in any
//! `completed_count`/pending-budget telemetry derived from
//! `get_swaps_by_status(["opening"])` or `inflight_swaps()` — even though
//! it will never open (the breaker blocks all further automation on it)
//! and the operator has already been told to intervene.
//!
//! **Fix.** `open.rs::maybe_trip_deadline_miss` (the Rust port of
//! `_maybe_trip_deadline_miss`) ALSO patches the row to a terminal status
//! (`"failed"`) in the same call that trips the breaker. This function
//! (`reserved_sats`) is unchanged from Python's SQL — the fix is entirely
//! in making sure an expired row is no longer in
//! [`crate::db_types::INFLIGHT_STATUSES`] by the time this sums it, not in
//! changing the summation rule itself.

use crate::db_types::{is_inflight_status, SwapRow};

/// `database.py::lnplus_reserved_sats` (7619-7629), ported verbatim as a
/// pure fold over already-fetched rows (the concrete `LnPlusDb`
/// implementation still owns doing this as a single SQL `SUM` for
/// production; this is the parity oracle for that query and what
/// `tests/telemetry.rs` proves against).
pub fn reserved_sats(rows: &[SwapRow]) -> i64 {
    rows.iter()
        .filter(|r| is_inflight_status(&r.status) && r.channel_funding_txid.is_none())
        .map(|r| r.capacity_sats)
        .sum()
}

/// Row counts by status, the shape `get_status()` (py 2114-2131) and any
/// `completed_count`-style dashboard telemetry ultimately reduces to.
pub fn count_by_status(rows: &[SwapRow], status: &str) -> usize {
    rows.iter().filter(|r| r.status == status).count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_types::SwapRow;

    fn row(sid: &str, status: &str, capacity: i64, funded: bool) -> SwapRow {
        let mut r = SwapRow::new(sid, status, capacity, 6, 0);
        if funded {
            r = r.with_channel_funding_txid("deadbeef");
        }
        r
    }

    #[test]
    fn reserved_sats_sums_unfunded_inflight_rows() {
        let rows = vec![
            row("a", "applied", 1_000_000, false),
            row("b", "opening", 2_000_000, false),
            row("c", "opened", 500_000, false),
        ];
        assert_eq!(reserved_sats(&rows), 3_500_000);
    }

    #[test]
    fn reserved_sats_excludes_funded_rows_b8() {
        // B8: once funded, the capacity already left listfunds -- counting
        // it again here would double-reserve it.
        let rows = vec![row("a", "opened", 1_000_000, true)];
        assert_eq!(reserved_sats(&rows), 0);
    }

    #[test]
    fn reserved_sats_excludes_terminal_rows() {
        let rows = vec![
            row("a", "ended", 1_000_000, false),
            row("b", "failed", 500_000, false),
        ];
        assert_eq!(reserved_sats(&rows), 0);
    }

    #[test]
    fn control_reserved_sats_would_double_count_a_still_opening_expired_row() {
        // CONTROL demonstrating the defect #4 shape this module documents:
        // a row LEFT at "opening" after a missed deadline (i.e. the bug --
        // status never terminalized) DOES still show up here. The actual
        // fix lives in `open.rs::maybe_trip_deadline_miss`, which must
        // transition the row to "failed" precisely so this sum excludes
        // it; this test proves `reserved_sats` alone cannot distinguish
        // "genuinely still trying to open" from "permanently stuck" --
        // that responsibility lives with the status transition, not the
        // summation.
        let rows = vec![row("expired", "opening", 5_000_000, false)];
        assert_eq!(
            reserved_sats(&rows),
            5_000_000,
            "a row stuck at 'opening' is indistinguishable from a live one here by design"
        );
    }

    #[test]
    fn count_by_status_basic() {
        let rows = vec![
            row("a", "opened", 1, false),
            row("b", "opened", 1, false),
            row("c", "applied", 1, false),
        ];
        assert_eq!(count_by_status(&rows, "opened"), 2);
        assert_eq!(count_by_status(&rows, "applied"), 1);
        assert_eq!(count_by_status(&rows, "ended"), 0);
    }
}
