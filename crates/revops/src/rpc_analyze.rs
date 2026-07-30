//! Pure response builder for `revenue-r-analyze`.
//!
//! Port of `revenue_analyze` (cl-revenue-ops.py:4517-4545), READ-ONLY
//! single-channel branch only: `flow_analyzer.analyze_channel(channel_id)`
//! -> [`FlowMetrics`] (ported byte-for-byte in `revops_analytics::flow`)
//! -> `.to_dict()`. The no-`channel_id` branch (`run_flow_analysis()`)
//! re-runs the WHOLE flow-analysis sweep and PERSISTS results to the DB --
//! a mutating background job, not read-only reporting, and out of this
//! batch's scope. Rather than fake Python's `{"status": "Flow analysis
//! triggered"}` (this builder performs no side effect, so claiming one was
//! triggered would be a lie -- see the project's honesty convention), that
//! branch returns an explicit `not_yet_ported` shape.

use revops_analytics::flow::FlowMetrics;
use serde_json::{json, Value};

/// `channel_id and not re.match(r'^\d+[x:]\d+[x:]\d+$', channel_id)`
/// (cl-revenue-ops.py:4532), ported as a manual scanner (no `regex` crate
/// in this workspace).
fn matches_scid_format(s: &str) -> bool {
    let mut parts = s.split(['x', ':']);
    let (Some(a), Some(b), Some(c)) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    if parts.next().is_some() {
        return false; // more than 3 parts
    }
    let is_digits = |p: &str| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit());
    is_digits(a) && is_digits(b) && is_digits(c)
}

/// `utils.normalize_scid`: replace `:` with `x`.
fn normalize_scid(s: &str) -> String {
    s.replace(':', "x")
}

/// Task 50 correction round, F5: whether the caller actually ran the
/// `FlowMetrics` assembly pipeline for this request. [`build_analyze`]'s
/// old behavior (always `None`, no marker) produced `{"channel": id,
/// "analysis": null}` for EVERY valid SCID -- byte-identical to Python's
/// legitimate *unknown/non-CHANNELD_NORMAL channel* answer
/// (cl-revenue-ops.py:4538). A live channel with real flow would read as
/// nonexistent. `NotWired` marks "the pipeline was never even attempted"
/// distinctly from `Ready(None)`, which is the genuine "pipeline ran, this
/// channel has no data" case Python's own shape already models correctly.
pub enum MetricsLookup<'a> {
    /// No `FlowMetrics` assembly pipeline exists yet -- this request never
    /// actually looked anything up. Must be marked, not silently `null`.
    NotWired,
    /// The pipeline ran (or would run, once wired); `None` here is a
    /// genuine "channel unknown to the flow analyzer" -- Python's own
    /// `{"channel": ..., "analysis": null}` shape, no marker needed.
    Ready(Option<&'a FlowMetrics>),
}

/// Port of `revenue_analyze`. `channel_id_raw` is the raw JSON param (kept
/// as `&Value`, not `&str`, to reproduce Python's `isinstance(channel_id,
/// str)` type gate against a caller that can pass any JSON type -- same
/// convention as `rpc_dashboard::parse_window_days`).
///
/// `metrics`: see [`MetricsLookup`] -- distinguishes "pipeline not wired"
/// from "pipeline ran, channel has no data" so the two cannot collide.
pub fn build_analyze(channel_id_raw: Option<&Value>, metrics: MetricsLookup) -> Value {
    let channel_id = match channel_id_raw {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.as_str()),
        Some(_) => {
            return json!({
                "error": "channel_id must be a string SCID (e.g., 123x456x789)."
            })
        }
    };

    // Python truthiness: `if channel_id and not re.match(...)` / `elif
    // channel_id:` -- an empty string behaves the same as absent.
    match channel_id.filter(|s| !s.is_empty()) {
        Some(id) => {
            if !matches_scid_format(id) {
                return json!({
                    "error": format!(
                        "Invalid channel format: {id}. Use SCID format (e.g., 123x456x789)."
                    )
                });
            }
            let normalized = normalize_scid(id);
            match metrics {
                MetricsLookup::NotWired => json!({
                    "channel": normalized,
                    "analysis": Value::Null,
                    "error": "not_yet_ported",
                }),
                MetricsLookup::Ready(m) => {
                    let analysis = m.map(flow_metrics_to_json).unwrap_or(Value::Null);
                    json!({"channel": normalized, "analysis": analysis})
                }
            }
        }
        None => json!({
            "error": "not_yet_ported",
            "reason": "whole-fleet flow analysis (run_flow_analysis) is a \
                       mutating background sweep, not read-only reporting; \
                       pass channel_id for the single-channel report",
        }),
    }
}

/// `FlowMetrics.to_dict()` (`FlowMetricsDict`) as JSON -- field names and
/// rounding exactly `modules/flow_analysis.py`'s `FlowMetrics.to_dict()`
/// (ported byte-for-byte in `revops_analytics::flow`; this is just the
/// `serde_json::Value` projection of that already-ported shape).
fn flow_metrics_to_json(m: &FlowMetrics) -> Value {
    let d = m.to_dict();
    json!({
        "channel_id": d.channel_id,
        "peer_id": d.peer_id,
        "sats_in": d.sats_in,
        "sats_out": d.sats_out,
        "capacity": d.capacity,
        "flow_ratio": d.flow_ratio,
        "state": d.state,
        "daily_volume": d.daily_volume,
        "is_congested": d.is_congested,
        "confidence": d.confidence,
        "velocity": d.velocity,
        "flow_multiplier": d.flow_multiplier,
        "ema_decay": d.ema_decay,
        "forward_count": d.forward_count,
        "kalman_flow_ratio": d.kalman_flow_ratio,
        "kalman_velocity": d.kalman_velocity,
        "kalman_uncertainty": d.kalman_uncertainty,
        "kalman_regime_change": d.kalman_regime_change,
    })
}

/// Task 67 slice 6b: serve a single channel's analysis from the flow
/// owner's PERSISTED state (`rust_channel_flow_states`).
///
/// The store holds the classification projection, not every `FlowMetrics`
/// field. The unpersisted fields are emitted as `null` and DECLARED in
/// `_gaps` — the project's convention, which the parity harness reads out
/// of the response to skip exactly those paths. Defaulting them to zero
/// would be indistinguishable from a genuinely idle channel, which is the
/// nullable-evidence failure this port keeps guarding against.
///
/// A channel with no row is Python's own `{"channel": ..., "analysis":
/// null}` — a real answer, not a marker and not an error.
pub fn build_analyze_from_persisted(
    channel_id_raw: Option<&Value>,
    row: Option<&revops_db::analytics::ChannelFlowStateRow>,
) -> Value {
    // Reuse the existing validation/normalization by delegating the
    // channel-id handling, then replace the analysis body.
    let shell = build_analyze(channel_id_raw, MetricsLookup::Ready(None));
    if shell.get("error").is_some() && shell.get("channel").is_none() {
        // A malformed channel_id (or the no-channel_id refusal) — return
        // that verdict untouched.
        return shell;
    }
    let Some(row) = row else {
        return shell;
    };
    let mut out = shell;
    out["analysis"] = json!({
        "peer_id": row.peer_id,
        "state": row.flow_state,
        "balance_position": row.balance_position,
        // F71-R20 split these apart: `flow_ratio`/`confidence` are the
        // EMA-side quantities, the `kalman_*` set is the filter's own
        // estimate. F71-R22: they are persisted now, so they are reported
        // now -- leaving them out would keep the surface claiming the
        // filter's output is unavailable when the row carries it.
        "flow_ratio": row.flow_ratio,
        "velocity": row.velocity,
        "confidence": row.confidence,
        "kalman_flow_ratio": row.kalman_flow_ratio,
        "kalman_velocity": row.kalman_velocity,
        "kalman_uncertainty": row.kalman_uncertainty,
        "kalman_regime_change": row.kalman_regime_change,
        "forward_count": row.forward_count,
        "updated_at": row.updated_at,
        "boot_id": row.boot_id,
        // Not persisted by the flow owner's projection. Declared, never
        // zeroed.
        "sats_in": Value::Null,
        "sats_out": Value::Null,
        "capacity": Value::Null,
        "daily_volume": Value::Null,
        "_gaps": ["sats_in", "sats_out", "capacity", "daily_volume"],
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use revops_analytics::classification::ChannelState;

    fn sample_metrics() -> FlowMetrics {
        FlowMetrics {
            channel_id: "123x456x789".to_string(),
            peer_id: "02".to_string() + &"b".repeat(64),
            sats_in: 100,
            sats_out: 200,
            capacity: 1_000_000,
            flow_ratio: 0.12345,
            state: ChannelState::Balanced,
            daily_volume: 5000,
            is_congested: false,
            confidence: 0.5,
            velocity: 0.001,
            flow_multiplier: 1.0,
            ema_decay: 0.8,
            forward_count: 12,
            kalman_flow_ratio: 0.1,
            kalman_velocity: 0.0,
            kalman_uncertainty: 0.0,
            kalman_regime_change: false,
        }
    }

    /// F71-R22: the Kalman fields are persisted by the flow owner now, so
    /// the analyze surface must REPORT them. Omitting them left the
    /// response implicitly claiming the filter's output was unavailable
    /// while the row carried it -- and left Task 66 with nothing to wire.
    ///
    /// The EMA and Kalman quantities must also stay DISTINCT here: F71-R20
    /// found `kalman_uncertainty` being written into `confidence`, its
    /// inverse, so this pins that the response does not re-merge them.
    #[test]
    fn persisted_kalman_fields_are_reported_and_stay_distinct_from_ema() {
        let row = revops_db::analytics::ChannelFlowStateRow {
            scid: "1x1x1".to_string(),
            peer_id: "02".to_string() + &"c".repeat(64),
            flow_state: "balanced".to_string(),
            balance_position: "balanced".to_string(),
            flow_ratio: 0.4,
            velocity: 0.02,
            confidence: 0.9,
            kalman_flow_ratio: -0.25,
            kalman_velocity: -0.01,
            kalman_uncertainty: 0.11,
            kalman_regime_change: true,
            forward_count: 7,
            updated_at: 1_800_000_000,
            boot_id: "boot-x".to_string(),
        };
        let v = build_analyze_from_persisted(Some(&json!("1x1x1")), Some(&row));
        let a = &v["analysis"];
        assert_eq!(a["kalman_flow_ratio"], json!(-0.25));
        assert_eq!(a["kalman_velocity"], json!(-0.01));
        assert_eq!(a["kalman_uncertainty"], json!(0.11));
        assert_eq!(a["kalman_regime_change"], json!(true));
        // The EMA-side pair is untouched and NOT equal to the Kalman pair.
        assert_eq!(a["flow_ratio"], json!(0.4));
        assert_eq!(a["confidence"], json!(0.9));
        assert_ne!(
            a["confidence"], a["kalman_uncertainty"],
            "confidence and uncertainty are inverses; reporting one as the \
             other inverts what the operator reads"
        );
        // The four genuinely-unpersisted fields stay declared gaps.
        assert_eq!(
            a["_gaps"],
            json!(["sats_in", "sats_out", "capacity", "daily_volume"]),
            "the kalman fields must NOT be listed as gaps now that they are \
             persisted and reported"
        );
    }

    #[test]
    fn missing_channel_id_is_documented_not_faked() {
        let v = build_analyze(None, MetricsLookup::NotWired);
        assert_eq!(v["error"], "not_yet_ported");
        // Control: must NOT claim Python's "Flow analysis triggered" --
        // this builder performs no side effect.
        assert_ne!(v.get("status"), Some(&json!("Flow analysis triggered")));
    }

    #[test]
    fn empty_string_channel_id_behaves_like_absent() {
        let v = build_analyze(Some(&json!("")), MetricsLookup::NotWired);
        assert_eq!(v["error"], "not_yet_ported");
    }

    #[test]
    fn non_string_channel_id_is_rejected() {
        let v = build_analyze(Some(&json!(123)), MetricsLookup::NotWired);
        assert_eq!(
            v["error"],
            "channel_id must be a string SCID (e.g., 123x456x789)."
        );
    }

    #[test]
    fn malformed_scid_is_rejected() {
        let v = build_analyze(Some(&json!("not-a-scid")), MetricsLookup::NotWired);
        assert_eq!(
            v["error"],
            "Invalid channel format: not-a-scid. Use SCID format (e.g., 123x456x789)."
        );
    }

    #[test]
    fn valid_scid_with_colon_separator_is_normalized_and_wrapped() {
        let m = sample_metrics();
        let v = build_analyze(Some(&json!("123:456:789")), MetricsLookup::Ready(Some(&m)));
        assert_eq!(v["channel"], "123x456x789");
        assert_eq!(v["analysis"]["channel_id"], "123x456x789");
        assert_eq!(v["analysis"]["state"], "balanced");
        assert_eq!(v["analysis"]["forward_count"], 12);
        // A genuinely-ready lookup carries no `not_yet_ported` marker.
        assert!(v.get("error").is_none());
    }

    #[test]
    fn valid_scid_with_no_data_yields_null_analysis_when_pipeline_is_ready() {
        let v = build_analyze(Some(&json!("1x1x1")), MetricsLookup::Ready(None));
        assert_eq!(v["channel"], "1x1x1");
        assert_eq!(v["analysis"], Value::Null);
        // Genuine "pipeline ran, channel unknown" -- Python's own shape,
        // no marker.
        assert!(v.get("error").is_none());
    }

    /// Task 50 correction round, F5: a valid SCID with the pipeline
    /// NOT-WIRED must carry the `not_yet_ported` marker so it cannot
    /// collide with the genuinely-ready-but-empty case above (a real live
    /// channel would otherwise read as nonexistent).
    #[test]
    fn valid_scid_not_wired_carries_marker_distinct_from_genuine_unknown() {
        let v = build_analyze(Some(&json!("1x1x1")), MetricsLookup::NotWired);
        assert_eq!(v["channel"], "1x1x1");
        assert_eq!(v["analysis"], Value::Null);
        assert_eq!(v["error"], "not_yet_ported");
    }
}
