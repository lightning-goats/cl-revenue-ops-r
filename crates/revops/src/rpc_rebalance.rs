//! `revenue-r-rebalance-plan` — the first production surface over the
//! `revops-rebalance` crate.
//!
//! # Why this module exists
//!
//! `revops-rebalance` is 8,167 lines that map 1:1 to Python's rebalance
//! stack, with tests for six of its eight components — and until
//! 2026-07-27 it was not even a `[dependencies]` entry of this crate. It
//! compiled as a workspace member and was never linked into the plugin
//! binary. That is the same "kernel ported, nothing calls it" failure the
//! fee stack carried in three places (see `docs/port/PARITY-CHECKLIST.md`
//! §2), and it is invisible to every test suite: a crate with no caller
//! passes all of its own tests.
//!
//! This is deliberately the READ-ONLY half. It runs the real planner over
//! live channel data and reports what it WOULD pair. It sends nothing:
//! no `sendpay`, no `askrene` reservation, no budget spend. Wiring the
//! execution half is a separate, tier-1 change — rebalancing moves real
//! sats, and unlike a fee change it is not reversible.

use revops_rebalance::planner::{plan, PlannerChannel};
use serde_json::{json, Value};

/// Default per-channel target band, used when no per-channel band is
/// resolved. Mirrors py `rebalance_utilization_floor`/`_ceiling`'s
/// balanced defaults; a channel inside its band is not a rebalance
/// candidate.
pub const DEFAULT_BAND_LOW: f64 = 0.25;
pub const DEFAULT_BAND_HIGH: f64 = 0.75;

/// Map one `listpeerchannels` entry to a [`PlannerChannel`].
///
/// Returns `None` for anything the planner must not consider: channels
/// that are not `CHANNELD_NORMAL`, and channels with no usable capacity.
/// Amounts arrive as msat (int or `"Nmsat"` string) and are floored to
/// sats, matching py `base_to_sats_floor`.
pub fn planner_channel_from_rpc(
    ch: &Value,
    band_low: f64,
    band_high: f64,
) -> Option<PlannerChannel> {
    if ch.get("state").and_then(Value::as_str) != Some("CHANNELD_NORMAL") {
        return None;
    }
    let channel_id = ch
        .get("short_channel_id")
        .and_then(Value::as_str)
        .map(crate::notify::normalize_scid)
        .filter(|s| !s.is_empty())?;
    let peer_id = ch
        .get("peer_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    let spendable_sats = msat_field_to_sats(ch, "spendable_msat");
    let receivable_sats = msat_field_to_sats(ch, "receivable_msat");
    let capacity_sats = match msat_field_to_sats(ch, "total_msat") {
        0 => spendable_sats + receivable_sats,
        n => n,
    };
    if capacity_sats <= 0 {
        return None;
    }

    Some(PlannerChannel {
        channel_id,
        peer_id,
        capacity_sats,
        spendable_sats,
        receivable_sats,
        band_low,
        band_high,
        // The remaining inputs are scoring refinements sourced from the
        // econ/analytics layers. This read-only surface reports the
        // BAND-driven plan only and pins them to neutral rather than
        // inventing values -- an invented score would make the reported
        // plan look better-informed than it is.
        inbound_ppm: 0,
        value_class: "neutral".to_string(),
        urgency: 0.0,
        drain: 0.0,
        capex_remaining_sats: 0,
    })
}

/// msat field (int or `"Nmsat"` string) floored to whole sats; `0` when
/// absent or unparseable.
fn msat_field_to_sats(ch: &Value, key: &str) -> i64 {
    let raw = match ch.get(key) {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
        Some(Value::String(s)) => s.trim_end_matches("msat").parse::<i64>().unwrap_or(0),
        _ => 0,
    };
    raw / 1000
}

/// Build the `revenue-r-rebalance-plan` response.
///
/// `_not_wired` is not decoration: this project's recurring defect is a
/// ported kernel with no caller, so any surface that exercises only part
/// of a subsystem says so in its own output rather than letting a caller
/// infer completeness.
pub fn build_rebalance_plan(
    channels: &[PlannerChannel],
    max_chunk_sats: i64,
    max_pairs: usize,
    pair_fee_cap_ppm: i64,
) -> Value {
    let out = plan(channels, max_chunk_sats, max_pairs, pair_fee_cap_ppm);
    json!({
        "schema_version": "revops_rebalance_plan/v1",
        "read_only": true,
        "considered_channels": channels.len(),
        "pairs": out.pairs.iter().map(|p| json!({
            "source": p.source,
            "dest": p.dest,
            "amount_sats": p.amount_sats,
            "pair_budget_sats": p.pair_budget_sats,
            "pair_fee_cap_ppm": p.pair_fee_cap_ppm,
            "score": p.score,
        })).collect::<Vec<_>>(),
        "skips": out.skips.iter().map(|s| json!({
            "channel_id": s.channel_id,
            "reason": s.reason,
        })).collect::<Vec<_>>(),
        "drain_demand": out.drain_demand.iter().map(|d| json!({
            "channel_id": d.channel_id,
            "peer_id": d.peer_id,
            "excess_sats": d.excess_sats,
            "drain_score": d.drain_score,
            "value_class": d.value_class,
        })).collect::<Vec<_>>(),
        "_not_wired": [
            "execution: no sendpay, no askrene reservation, no budget spend",
            "scoring inputs inbound_ppm/value_class/urgency/drain/capex pinned neutral",
            "no rebalance loop: this surface runs only when the RPC is called"
        ],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ch(scid: &str, state: &str, spend: i64, recv: i64) -> Value {
        json!({
            "short_channel_id": scid,
            "peer_id": "03aa",
            "state": state,
            "spendable_msat": spend * 1000,
            "receivable_msat": recv * 1000,
            "total_msat": (spend + recv) * 1000,
        })
    }

    #[test]
    fn only_normal_channels_are_planner_candidates() {
        let normal = ch("1x1x0", "CHANNELD_NORMAL", 900_000, 100_000);
        assert!(planner_channel_from_rpc(&normal, 0.25, 0.75).is_some());

        // Control: identical channel, non-normal state.
        let closing = ch("1x1x0", "CHANNELD_SHUTTING_DOWN", 900_000, 100_000);
        assert!(planner_channel_from_rpc(&closing, 0.25, 0.75).is_none());
    }

    #[test]
    fn msat_amounts_are_floored_to_sats_from_either_shape() {
        let numeric = ch("1x1x0", "CHANNELD_NORMAL", 1_500, 500);
        let c = planner_channel_from_rpc(&numeric, 0.25, 0.75).unwrap();
        assert_eq!(c.spendable_sats, 1_500);
        assert_eq!(c.capacity_sats, 2_000);

        let stringy = json!({
            "short_channel_id": "2x2x0", "peer_id": "03bb", "state": "CHANNELD_NORMAL",
            "spendable_msat": "1999msat", "receivable_msat": "1msat", "total_msat": "2000msat",
        });
        let c2 = planner_channel_from_rpc(&stringy, 0.25, 0.75).unwrap();
        assert_eq!(
            c2.spendable_sats, 1,
            "1999 msat floors to 1 sat, never rounds up"
        );
    }

    #[test]
    fn a_zero_capacity_channel_is_refused() {
        let empty = ch("1x1x0", "CHANNELD_NORMAL", 0, 0);
        assert!(planner_channel_from_rpc(&empty, 0.25, 0.75).is_none());
    }

    /// The surface actually runs the ported planner: a lopsided pair must
    /// produce a plan, and a balanced one must not.
    #[test]
    fn the_planner_pairs_an_over_local_with_an_over_remote_channel() {
        let src =
            planner_channel_from_rpc(&ch("1x1x0", "CHANNELD_NORMAL", 950_000, 50_000), 0.25, 0.75)
                .unwrap();
        let mut dst =
            planner_channel_from_rpc(&ch("2x2x0", "CHANNELD_NORMAL", 50_000, 950_000), 0.25, 0.75)
                .unwrap();
        dst.peer_id = "03bb".to_string();

        let v = build_rebalance_plan(&[src, dst], 200_000, 4, 1_000);
        assert_eq!(v["read_only"], true);
        assert_eq!(v["considered_channels"], 2);
        assert!(
            !v["pairs"].as_array().unwrap().is_empty(),
            "an over-local and an over-remote channel on different peers must pair: {v}"
        );
    }

    /// Control for the test above: both channels inside their band means
    /// no pairs, proving the assertion above can fail.
    #[test]
    fn balanced_channels_produce_no_pairs() {
        let a = planner_channel_from_rpc(
            &ch("1x1x0", "CHANNELD_NORMAL", 500_000, 500_000),
            0.25,
            0.75,
        )
        .unwrap();
        let mut b = planner_channel_from_rpc(
            &ch("2x2x0", "CHANNELD_NORMAL", 500_000, 500_000),
            0.25,
            0.75,
        )
        .unwrap();
        b.peer_id = "03bb".to_string();

        let v = build_rebalance_plan(&[a, b], 200_000, 4, 1_000);
        assert!(v["pairs"].as_array().unwrap().is_empty());
        assert_eq!(
            v["skips"].as_array().unwrap().len(),
            2,
            "both are inside_band"
        );
    }

    /// The response must never imply the execution half is wired.
    #[test]
    fn the_response_declares_what_is_not_wired() {
        let v = build_rebalance_plan(&[], 200_000, 4, 1_000);
        let not_wired = v["_not_wired"].as_array().unwrap();
        assert!(!not_wired.is_empty());
        assert!(
            not_wired
                .iter()
                .any(|s| s.as_str().unwrap().contains("sendpay")),
            "the absence of execution must be stated in the payload itself"
        );
    }
}
