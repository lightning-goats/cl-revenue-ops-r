//! Task 71 / F71-R27: the LIVE flow-analysis config resolver.
//!
//! `FlowPassConfig::default()` froze py's defaults into the binary at
//! construction time. An operator who sets `revenue-ops-flow-interval` or
//! writes a `source_threshold` override sees nothing change until the
//! plugin restarts — and worse, the values silently disagree with what
//! `revenue-r-config` reports, so the config surface becomes a lie about
//! what the classifier is actually using.
//!
//! Python resolves these per pass. This resolves them per pass too, in
//! py's own precedence order: **DB override > listconfigs > default**.
//!
//! **Two of the five have no listconfigs tier, and that is not an
//! oversight.** `flow_interval`, `flow_window_days` and
//! `htlc_congestion_threshold` are registered plugin options
//! (`revenue-ops-flow-interval` etc., cl-revenue-ops.py:1520/2512/2621).
//! `source_threshold` and `sink_threshold` are NOT — they exist only as
//! `Config` fields with DB-override support (config.py:597-598). Inventing
//! listconfigs keys for them would mean reading names lightningd will
//! never hold, and quietly reporting "resolved from listconfigs" for a
//! tier that does not exist.
//!
//! Every failure is a typed refusal. A `config_overrides` read that FAILS
//! is not "no override" — resolving past it would silently run the
//! operator's node on defaults they explicitly replaced.

use std::collections::BTreeMap;

/// py `config.py:589,597,598,743` and `505`.
pub const DEFAULT_SOURCE_THRESHOLD: f64 = 0.05;
pub const DEFAULT_SINK_THRESHOLD: f64 = -0.05;
pub const DEFAULT_FLOW_WINDOW_DAYS: i64 = 7;
pub const DEFAULT_HTLC_CONGESTION_THRESHOLD: f64 = 0.8;
pub const DEFAULT_FLOW_INTERVAL_SECONDS: i64 = 3_600;

/// py `config.py:402,410,391` validation ranges.
const FLOW_INTERVAL_RANGE: (i64, i64) = (60, 86_400);
const FLOW_WINDOW_DAYS_RANGE: (i64, i64) = (1, 365);
const THRESHOLD_RANGE: (f64, f64) = (-1.0, 1.0);
const HTLC_THRESHOLD_RANGE: (f64, f64) = (0.0, 1.0);

/// Where each value came from, so the config surface can report it
/// truthfully instead of claiming everything is a default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueSource {
    DbOverride,
    ListConfigs,
    Default,
}

/// Already-read config evidence. Both sources arrive `Result`/map-shaped
/// from the caller's real reads, so every failure path is drivable.
pub struct FlowConfigSources {
    /// `config_overrides` rows, keyed by `Config` FIELD name
    /// (`source_threshold`, not `revenue-ops-source-threshold`). `Err`
    /// means the read failed and resolution must refuse.
    pub db_overrides: Result<BTreeMap<String, String>, String>,
    /// The `listconfigs` snapshot, keyed by FULL option name.
    pub listconfigs: BTreeMap<String, String>,
}

/// One resolved pass's tunables, each with its provenance.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ResolvedFlowConfig {
    pub source_threshold: f64,
    pub sink_threshold: f64,
    pub flow_window_days: i64,
    pub htlc_congestion_threshold: f64,
    pub flow_interval_seconds: i64,
    pub source_threshold_from: ValueSource,
    pub sink_threshold_from: ValueSource,
    pub flow_window_days_from: ValueSource,
    pub htlc_congestion_threshold_from: ValueSource,
    pub flow_interval_from: ValueSource,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FlowConfigRefusal {
    /// The `config_overrides` read itself failed.
    OverridesUnavailable(String),
    /// A present value could not be parsed as its declared type.
    Unparseable { key: String, raw: String },
    /// A parsed value is outside py's declared range.
    OutOfRange { key: String, raw: String },
    /// py `config.py:1143-1146`: sink must be strictly below source.
    InvertedBand { source: f64, sink: f64 },
}

impl FlowConfigRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::OverridesUnavailable(_) => "flow_config_overrides_unavailable",
            Self::Unparseable { .. } => "flow_config_unparseable",
            Self::OutOfRange { .. } => "flow_config_out_of_range",
            Self::InvertedBand { .. } => "flow_config_inverted_band",
        }
    }
}

/// Look up one key across the tiers. `option_name` is `None` for the two
/// fields lightningd never holds.
fn tiered<'a>(
    sources: &'a BTreeMap<String, String>,
    listconfigs: &'a BTreeMap<String, String>,
    field: &str,
    option_name: Option<&str>,
) -> Option<(&'a str, ValueSource)> {
    if let Some(raw) = sources.get(field) {
        return Some((raw.as_str(), ValueSource::DbOverride));
    }
    if let Some(name) = option_name {
        if let Some(raw) = listconfigs.get(name) {
            return Some((raw.as_str(), ValueSource::ListConfigs));
        }
    }
    None
}

fn resolve_f64(
    overrides: &BTreeMap<String, String>,
    listconfigs: &BTreeMap<String, String>,
    field: &str,
    option_name: Option<&str>,
    default: f64,
    range: (f64, f64),
) -> Result<(f64, ValueSource), FlowConfigRefusal> {
    let Some((raw, from)) = tiered(overrides, listconfigs, field, option_name) else {
        return Ok((default, ValueSource::Default));
    };
    let parsed = raw
        .trim()
        .parse::<f64>()
        .map_err(|_| FlowConfigRefusal::Unparseable {
            key: field.to_string(),
            raw: raw.to_string(),
        })?;
    if !parsed.is_finite() || parsed < range.0 || parsed > range.1 {
        return Err(FlowConfigRefusal::OutOfRange {
            key: field.to_string(),
            raw: raw.to_string(),
        });
    }
    Ok((parsed, from))
}

fn resolve_i64(
    overrides: &BTreeMap<String, String>,
    listconfigs: &BTreeMap<String, String>,
    field: &str,
    option_name: Option<&str>,
    default: i64,
    range: (i64, i64),
) -> Result<(i64, ValueSource), FlowConfigRefusal> {
    let Some((raw, from)) = tiered(overrides, listconfigs, field, option_name) else {
        return Ok((default, ValueSource::Default));
    };
    let parsed = raw
        .trim()
        .parse::<i64>()
        .map_err(|_| FlowConfigRefusal::Unparseable {
            key: field.to_string(),
            raw: raw.to_string(),
        })?;
    if parsed < range.0 || parsed > range.1 {
        return Err(FlowConfigRefusal::OutOfRange {
            key: field.to_string(),
            raw: raw.to_string(),
        });
    }
    Ok((parsed, from))
}

/// Resolve every flow tunable for ONE pass.
pub fn resolve_flow_config(
    sources: FlowConfigSources,
) -> Result<ResolvedFlowConfig, FlowConfigRefusal> {
    let overrides = sources
        .db_overrides
        .map_err(FlowConfigRefusal::OverridesUnavailable)?;
    let lc = sources.listconfigs;

    let (source_threshold, source_threshold_from) = resolve_f64(
        &overrides,
        &lc,
        "source_threshold",
        None,
        DEFAULT_SOURCE_THRESHOLD,
        THRESHOLD_RANGE,
    )?;
    let (sink_threshold, sink_threshold_from) = resolve_f64(
        &overrides,
        &lc,
        "sink_threshold",
        None,
        DEFAULT_SINK_THRESHOLD,
        THRESHOLD_RANGE,
    )?;
    let (flow_window_days, flow_window_days_from) = resolve_i64(
        &overrides,
        &lc,
        "flow_window_days",
        Some("revenue-ops-flow-window-days"),
        DEFAULT_FLOW_WINDOW_DAYS,
        FLOW_WINDOW_DAYS_RANGE,
    )?;
    let (htlc_congestion_threshold, htlc_congestion_threshold_from) = resolve_f64(
        &overrides,
        &lc,
        "htlc_congestion_threshold",
        Some("revenue-ops-htlc-congestion-threshold"),
        DEFAULT_HTLC_CONGESTION_THRESHOLD,
        HTLC_THRESHOLD_RANGE,
    )?;
    let (flow_interval_seconds, flow_interval_from) = resolve_i64(
        &overrides,
        &lc,
        "flow_interval",
        Some("revenue-ops-flow-interval"),
        DEFAULT_FLOW_INTERVAL_SECONDS,
        FLOW_INTERVAL_RANGE,
    )?;

    // py config.py:1143-1146 rejects the mutation that would invert the
    // band. A band where sink >= source is not merely odd: every ratio
    // lands in BOTH the source and sink tests, and the first branch wins,
    // so a whole fleet classifies SOURCE regardless of measured flow.
    if sink_threshold >= source_threshold {
        return Err(FlowConfigRefusal::InvertedBand {
            source: source_threshold,
            sink: sink_threshold,
        });
    }

    Ok(ResolvedFlowConfig {
        source_threshold,
        sink_threshold,
        flow_window_days,
        htlc_congestion_threshold,
        flow_interval_seconds,
        source_threshold_from,
        sink_threshold_from,
        flow_window_days_from,
        htlc_congestion_threshold_from,
        flow_interval_from,
    })
}
