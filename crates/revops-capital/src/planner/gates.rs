//! Port of two F8-audit capital-concentration guards from `CapacityPlanner`
//! (py `modules/capacity_planner.py`): `_failed_open_backoff_reason`
//! (2410-2457) and `_peer_exposure_cap_reason` (2459-2504).

use revops_core::msat::base_to_sats_floor;

/// py `FAILED_OPEN_BACKOFF_CAP_HOURS = 168` (capacity_planner.py 56).
pub const FAILED_OPEN_BACKOFF_CAP_HOURS: f64 = 168.0;
/// py `PEER_EXPOSURE_CAP_MULTIPLIER = 2` (capacity_planner.py 60).
pub const PEER_EXPOSURE_CAP_MULTIPLIER: i64 = 2;

/// One `planner_actions` row as `_failed_open_backoff_reason` reads it (py
/// 2431-2445): `action_type`, `status`, `created_at`. Order does not need
/// to be "newest first" for [`failed_open_backoff_reason`] alone (it takes
/// the max failure timestamp), but Python's early `break` on a
/// `completed`/`dry_run` open DOES depend on descending order — pass
/// `actions` pre-sorted newest-first (as `ORDER BY created_at DESC`, the
/// Python query's contract) to preserve that reset semantics exactly.
#[derive(Debug, Clone, Copy)]
pub struct PlannerActionRecord<'a> {
    pub action_type: &'a str,
    pub status: &'a str,
    pub created_at: i64,
}

/// Port of `_failed_open_backoff_reason` (py capacity_planner.py 2410-2457):
/// skip candidates in exponential failed-open backoff. Counts the peer's
/// consecutive most-recent FAILED open actions within the
/// [`FAILED_OPEN_BACKOFF_CAP_HOURS`] window (a completed/dry_run open
/// resets the streak via the early `break`). With N failures, the peer is
/// skipped until `2^N` hours (capped) have elapsed since the latest
/// failure. `now` is the injected clock (py `time.time()`, 2451).
pub fn failed_open_backoff_reason(
    peer_id: &str,
    actions: &[PlannerActionRecord],
    now: i64,
) -> Option<String> {
    let mut failures: i32 = 0;
    let mut last_failure_ts: i64 = 0;

    for action in actions {
        if action.action_type != "open" {
            continue;
        }
        let status = action.status.to_lowercase();
        if status == "failed" {
            failures += 1;
            last_failure_ts = last_failure_ts.max(action.created_at.max(0));
        } else if status == "completed" || status == "dry_run" {
            break; // a successful open resets the failure streak
        }
    }

    if failures <= 0 || last_failure_ts <= 0 {
        return None;
    }

    let backoff_hours = 2f64
        .powi(failures.min(30))
        .min(FAILED_OPEN_BACKOFF_CAP_HOURS);
    let elapsed_hours = ((now - last_failure_ts) as f64 / 3600.0).max(0.0);
    if elapsed_hours < backoff_hours {
        let peer_display: String = peer_id.chars().take(16).collect();
        return Some(format!(
            "Failed-open backoff for {peer_display}...: {failures} recent \
             failure(s), retry in {:.1}h",
            backoff_hours - elapsed_hours
        ));
    }
    None
}

/// One `listpeerchannels` row's fields `_peer_exposure_cap_reason` reads
/// (py 2489-2496).
#[derive(Debug, Clone, Copy)]
pub struct PeerChannelCapacity<'a> {
    pub peer_id: &'a str,
    pub state: &'a str,
    pub total_msat: i64,
}

/// Port of `_peer_exposure_cap_reason` (py capacity_planner.py 2459-2504):
/// skip opens that would over-concentrate capital on one peer. `channels`
/// is the (already-fetched) `listpeerchannels` snapshot (py's
/// `self._cycle_peer_channels` cache, reused across the cycle to avoid an
/// extra RPC in the hot loop). `max_channel_sats <= 0` disables the gate
/// (py 2469-2474, `cap_sats <= 0` check after multiplying).
pub fn peer_exposure_cap_reason(
    peer_id: &str,
    max_channel_sats: i64,
    channels: &[PeerChannelCapacity],
) -> Option<String> {
    let cap_sats = max_channel_sats * PEER_EXPOSURE_CAP_MULTIPLIER;
    if cap_sats <= 0 {
        return None;
    }

    let mut total_capacity_sats: i64 = 0;
    for channel in channels {
        if channel.peer_id != peer_id {
            continue;
        }
        if channel.state != "CHANNELD_NORMAL" {
            continue;
        }
        total_capacity_sats += base_to_sats_floor(channel.total_msat.max(0) as u64) as i64;
    }

    if total_capacity_sats >= cap_sats {
        let peer_display: String = peer_id.chars().take(16).collect();
        return Some(format!(
            "Peer exposure cap for {peer_display}...: existing capacity \
             {total_capacity_sats} sats >= cap {cap_sats} sats \
             ({PEER_EXPOSURE_CAP_MULTIPLIER}x planner_max_channel_sats)"
        ));
    }
    None
}
