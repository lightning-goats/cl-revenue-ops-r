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
