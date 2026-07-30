//! Task 71 / F71-R27: the live flow-config resolver.

use std::collections::BTreeMap;

use revops::flow_config::{
    resolve_flow_config, FlowConfigRefusal, FlowConfigSources, ValueSource,
    DEFAULT_FLOW_INTERVAL_SECONDS, DEFAULT_SOURCE_THRESHOLD,
};

fn sources(db: &[(&str, &str)], lc: &[(&str, &str)]) -> FlowConfigSources {
    FlowConfigSources {
        db_overrides: Ok(db
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()),
        listconfigs: lc
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    }
}

/// With nothing configured anywhere, py's defaults stand — and say so.
#[test]
fn nothing_configured_resolves_to_python_defaults() {
    let c = resolve_flow_config(sources(&[], &[])).expect("defaults resolve");
    assert_eq!(c.source_threshold, DEFAULT_SOURCE_THRESHOLD);
    assert_eq!(c.sink_threshold, -0.05);
    assert_eq!(c.flow_window_days, 7);
    assert_eq!(c.htlc_congestion_threshold, 0.8);
    assert_eq!(c.flow_interval_seconds, DEFAULT_FLOW_INTERVAL_SECONDS);
    assert_eq!(c.flow_interval_from, ValueSource::Default);
}

/// The whole point of R27: a non-default value must actually REACH the
/// classifier and the next cadence, not sit in the database unread.
#[test]
fn non_default_values_reach_the_classifier_and_the_cadence() {
    let c = resolve_flow_config(sources(
        &[("source_threshold", "0.25"), ("sink_threshold", "-0.3")],
        &[
            ("revenue-ops-flow-interval", "900"),
            ("revenue-ops-flow-window-days", "14"),
            ("revenue-ops-htlc-congestion-threshold", "0.6"),
        ],
    ))
    .expect("configured values resolve");

    assert_eq!(c.source_threshold, 0.25);
    assert_eq!(c.sink_threshold, -0.3);
    assert_eq!(c.flow_window_days, 14);
    assert_eq!(c.htlc_congestion_threshold, 0.6);
    assert_eq!(
        c.flow_interval_seconds, 900,
        "the NEXT cadence must use the resolved interval, not the default"
    );
    assert_eq!(c.source_threshold_from, ValueSource::DbOverride);
    assert_eq!(c.flow_interval_from, ValueSource::ListConfigs);
}

/// py precedence: a DB override beats lightningd's resolved value.
#[test]
fn a_db_override_beats_listconfigs() {
    let c = resolve_flow_config(sources(
        &[("flow_interval", "1200")],
        &[("revenue-ops-flow-interval", "900")],
    ))
    .expect("resolve");
    assert_eq!(c.flow_interval_seconds, 1200);
    assert_eq!(c.flow_interval_from, ValueSource::DbOverride);
}

/// `source_threshold` and `sink_threshold` are NOT plugin options — they
/// exist only as Config fields. A listconfigs entry under an invented name
/// must not be honoured, or the resolver would be reading a key
/// lightningd never holds and mislabelling its provenance.
#[test]
fn thresholds_have_no_listconfigs_tier() {
    let c = resolve_flow_config(sources(
        &[],
        &[
            ("revenue-ops-source-threshold", "0.9"),
            ("revenue-ops-sink-threshold", "-0.9"),
        ],
    ))
    .expect("resolve");
    assert_eq!(c.source_threshold, DEFAULT_SOURCE_THRESHOLD);
    assert_eq!(c.source_threshold_from, ValueSource::Default);
    assert_eq!(c.sink_threshold_from, ValueSource::Default);
}

/// A FAILED `config_overrides` read is not "no override". Resolving past
/// it would silently run the node on defaults the operator replaced.
#[test]
fn a_failed_override_read_refuses_rather_than_defaulting() {
    let err = resolve_flow_config(FlowConfigSources {
        db_overrides: Err("config_overrides read failed: disk i/o error".into()),
        listconfigs: BTreeMap::new(),
    })
    .expect_err("a failed read must refuse");
    assert_eq!(err.code(), "flow_config_overrides_unavailable");
}

/// py `config.py:1143-1146` rejects an inverted band. It matters: every
/// ratio would satisfy BOTH the source and sink tests, the first branch
/// wins, and the whole fleet classifies SOURCE regardless of real flow.
#[test]
fn an_inverted_band_refuses() {
    let err = resolve_flow_config(sources(
        &[("source_threshold", "-0.2"), ("sink_threshold", "0.2")],
        &[],
    ))
    .expect_err("sink above source must refuse");
    assert!(matches!(err, FlowConfigRefusal::InvertedBand { .. }));
    assert_eq!(err.code(), "flow_config_inverted_band");

    // Equal is also rejected: the bands must not touch.
    let err = resolve_flow_config(sources(
        &[("source_threshold", "0.1"), ("sink_threshold", "0.1")],
        &[],
    ))
    .expect_err("equal thresholds must refuse");
    assert_eq!(err.code(), "flow_config_inverted_band");
}

/// A present-but-unparseable value refuses instead of falling back — the
/// same rule the money boundaries follow. A typo'd override that silently
/// became the default is indistinguishable from never having set it.
#[test]
fn unparseable_and_out_of_range_values_refuse() {
    let err = resolve_flow_config(sources(&[("flow_interval", "soon")], &[]))
        .expect_err("unparseable refuses");
    assert_eq!(err.code(), "flow_config_unparseable");

    // py range for flow_interval is (60, 86400).
    let err = resolve_flow_config(sources(&[("flow_interval", "5")], &[]))
        .expect_err("below py's range refuses");
    assert_eq!(err.code(), "flow_config_out_of_range");

    // py range for htlc_congestion_threshold is (0.0, 1.0).
    let err = resolve_flow_config(sources(&[("htlc_congestion_threshold", "1.5")], &[]))
        .expect_err("above py's range refuses");
    assert_eq!(err.code(), "flow_config_out_of_range");
}

/// py's own `int()`/`float()` accept surrounding whitespace, and the
/// override validator preserves padded rows, so a padded value must
/// resolve rather than refuse.
#[test]
fn padded_values_parse_like_python() {
    let c = resolve_flow_config(sources(&[("flow_interval", "  900  ")], &[]))
        .expect("padded value resolves");
    assert_eq!(c.flow_interval_seconds, 900);
}
