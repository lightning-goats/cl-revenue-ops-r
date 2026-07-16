//! Demand-flow peer/candidate classifier (port of `modules/demand_flow.py`,
//! whole file — Task 7, Wave 2).
//!
//! Classifies network nodes as sources/sinks/routers from internal flow
//! data and gossip heuristics, feeding the capacity planner's channel-open
//! candidate scoring. Pure: no RPC, no DB, no clock (gossip payloads and
//! `existing_peers` are all caller-supplied evidence).

use std::collections::{HashMap, HashSet};

use revops_core::msat::parse_msat;
use revops_econ::pyfloat::{py_repr, py_round};
use serde_json::Value;

use crate::flow::FlowMetrics;

pub const EXCHANGE_KEYWORDS: &[&str] = &[
    "kraken",
    "coinbase",
    "okx",
    "bitfinex",
    "binance",
    "bitstamp",
    "nicehash",
    "river",
    "strike",
    "cashapp",
    "robinhood",
    "gemini",
    "bitget",
    "bybit",
    "kucoin",
    "huobi",
    "gate.io",
];

pub const SINK_KEYWORDS: &[&str] = &[
    "wallet", "pay", "shop", "store", "merchant", "pos", "btcpay", "coinos", "zebedee", "fountain",
    "stacker",
];

pub const LSP_KEYWORDS: &[&str] = &[
    "lnbig",
    "lqwd",
    "acinq",
    "breez",
    "phoenix",
    "muun",
    "olympus",
    "voltage",
    "greenlight",
];

/// Port of `NodeFlowProfile`. `gossip_signals` is a `Vec<(String, f64)>`
/// rather than a map to preserve Python dict insertion order (never
/// iterated for correctness here, but kept for parity/debuggability).
#[derive(Clone, Debug, PartialEq)]
pub struct NodeFlowProfile {
    pub node_id: String,
    pub role: String,
    pub confidence: f64,
    pub net_flow_ratio: Option<f64>,
    pub gossip_signals: Vec<(String, f64)>,
    pub has_liquidity_ads: bool,
}

impl NodeFlowProfile {
    fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            role: "unknown".to_string(),
            confidence: 0.0,
            net_flow_ratio: None,
            gossip_signals: Vec::new(),
            has_liquidity_ads: false,
        }
    }
}

/// `_safe_float`: convert loose RPC/gossip values to `f64` without letting
/// bad data abort scoring. `None`/missing-key callers pass `None` directly.
fn safe_float(value: Option<&Value>, default: f64) -> f64 {
    match value {
        None | Some(Value::Null) | Some(Value::Bool(_)) => default,
        Some(Value::Number(n)) => n.as_f64().unwrap_or(default),
        Some(Value::String(s)) => s.parse::<f64>().unwrap_or(default),
        _ => default,
    }
}

/// `ch.get(key, default)` then `_safe_float(..., default)` collapsed into
/// one call — the Python default value flows through unconditionally when
/// the key is absent (never re-validated), and through `_safe_float`'s
/// error fallback when present-but-unparseable.
fn get_f64_or(ch: &Value, key: &str, default: f64) -> f64 {
    match ch.get(key) {
        None => default,
        some => safe_float(some, default),
    }
}

/// `parse_msat(ch.get(key, default_msat))`: dict-get-with-default, then
/// msat coercion. The literal default never itself goes through
/// `parse_msat` in Python (`dict.get` just returns it directly) but for an
/// int default that is a no-op either way.
fn get_msat_or(ch: &Value, key: &str, default_msat: i64) -> i64 {
    match ch.get(key) {
        None => default_msat,
        Some(v) => parse_msat(v),
    }
}

/// Python `bool(x)` truthiness over a JSON value (used for `ch.get("active",
/// False)` and `node_info["option_will_fund"]`).
fn json_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

fn ch_active(ch: &Value) -> bool {
    ch.is_object() && ch.get("active").map(json_truthy).unwrap_or(false)
}

/// Port of `DemandFlowClassifier.classify_peers`. Returns a `BTreeMap`
/// (deterministic order) rather than the Python `Dict` — the Python
/// implementation's own iteration order comes from a `set` union and is
/// unspecified/hash-order-dependent, so no behavior is lost by picking a
/// canonical (sorted-by-`node_id`) order here.
pub fn classify_peers(
    all_flow: &[FlowMetrics],
) -> std::collections::BTreeMap<String, NodeFlowProfile> {
    let mut peer_in: HashMap<String, i64> = HashMap::new();
    let mut peer_out: HashMap<String, i64> = HashMap::new();

    for flow in all_flow {
        if flow.peer_id.is_empty() {
            continue;
        }
        *peer_in.entry(flow.peer_id.clone()).or_insert(0) += flow.sats_in;
        *peer_out.entry(flow.peer_id.clone()).or_insert(0) += flow.sats_out;
    }

    let mut pids: HashSet<&String> = peer_in.keys().collect();
    pids.extend(peer_out.keys());

    let mut profiles = std::collections::BTreeMap::new();
    for pid in pids {
        let total_in = *peer_in.get(pid).unwrap_or(&0);
        let total_out = *peer_out.get(pid).unwrap_or(&0);
        let total = total_in + total_out;

        if total == 0 {
            profiles.insert(pid.clone(), NodeFlowProfile::new(pid.clone()));
            continue;
        }

        let ratio = (total_in - total_out) as f64 / total as f64;
        let role = if ratio > 0.3 {
            "source"
        } else if ratio < -0.3 {
            "sink"
        } else {
            "router"
        };

        let confidence = f64::min(
            0.9,
            0.3 * f64::max(total as f64, 1.0).log10() / 1_000_000f64.log10(),
        );
        let confidence = f64::max(0.1, confidence);

        profiles.insert(
            pid.clone(),
            NodeFlowProfile {
                node_id: pid.clone(),
                role: role.to_string(),
                confidence: py_round(confidence, 3),
                net_flow_ratio: Some(py_round(ratio, 4)),
                gossip_signals: Vec::new(),
                has_liquidity_ads: false,
            },
        );
    }

    profiles
}

/// Port of `DemandFlowClassifier.classify_candidate`. `channels` mirrors
/// Python's `channels or []` default (pass an empty slice for `None`).
pub fn classify_candidate(
    node_id: &str,
    node_info: Option<&Value>,
    channels: &[Value],
) -> NodeFlowProfile {
    let empty_info = Value::Object(serde_json::Map::new());
    let node_info = node_info.unwrap_or(&empty_info);

    let alias = node_info
        .get("alias")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let alias_lower = alias.to_lowercase();

    let mut signals: Vec<(String, f64)> = Vec::new();
    let mut source_score = 0.0_f64;
    let mut sink_score = 0.0_f64;
    let mut router_score = 0.0_f64;

    // Heuristic 1: alias pattern matching.
    if EXCHANGE_KEYWORDS.iter().any(|kw| alias_lower.contains(kw)) {
        signals.push(("alias_exchange".to_string(), 0.6));
        source_score += 0.6;
    }
    if SINK_KEYWORDS.iter().any(|kw| alias_lower.contains(kw)) {
        signals.push(("alias_sink".to_string(), 0.5));
        sink_score += 0.5;
    }
    if LSP_KEYWORDS.iter().any(|kw| alias_lower.contains(kw)) {
        signals.push(("alias_lsp".to_string(), 0.4));
        router_score += 0.4;
    }

    let active: Vec<&Value> = channels.iter().filter(|ch| ch_active(ch)).collect();

    // Heuristic 2: channel structure analysis.
    if !active.is_empty() {
        let count = active.len() as i64;
        let total_cap_msat: i64 = active
            .iter()
            .map(|ch| std::cmp::max(0, get_msat_or(ch, "amount_msat", 0)))
            .sum();
        let total_cap_btc = total_cap_msat as f64 / 100_000_000_000.0;
        let avg_cap = if count != 0 {
            total_cap_msat / count
        } else {
            0
        };

        if count > 100 && total_cap_btc > 5.0 {
            signals.push(("structure_hub".to_string(), 0.5));
            router_score += 0.5;
        } else if count > 30 && avg_cap < 500_000_000 {
            signals.push(("structure_sink".to_string(), 0.4));
            sink_score += 0.4;
        } else if count < 10 && avg_cap > 5_000_000_000 {
            signals.push(("structure_source".to_string(), 0.3));
            source_score += 0.3;
        }

        // Heuristic 3: fee policy.
        let low_fee_count = active
            .iter()
            .filter(|ch| {
                get_msat_or(ch, "base_fee_millisatoshi", 1000) == 0
                    && get_f64_or(ch, "fee_per_millionth", 1000.0) < 50.0
            })
            .count();
        if (low_fee_count as f64) > (active.len() as f64) * 0.5 {
            signals.push(("fee_sink".to_string(), 0.3));
            sink_score += 0.3;
        }

        let high_fee_count = active
            .iter()
            .filter(|ch| get_f64_or(ch, "fee_per_millionth", 0.0) > 500.0)
            .count();
        if (high_fee_count as f64) > (active.len() as f64) * 0.5 {
            signals.push(("fee_extractive".to_string(), -0.2));
        }
    }

    // Heuristic 4: liquidity ads.
    let has_lads = node_info
        .get("option_will_fund")
        .map(json_truthy)
        .unwrap_or(false);

    let total = source_score + sink_score + router_score;
    let (role, confidence) = if total == 0.0 {
        ("unknown".to_string(), 0.0)
    } else if source_score >= sink_score && source_score >= router_score {
        ("source".to_string(), source_score / total)
    } else if sink_score >= source_score && sink_score >= router_score {
        ("sink".to_string(), sink_score / total)
    } else {
        ("router".to_string(), router_score / total)
    };

    NodeFlowProfile {
        node_id: node_id.to_string(),
        role,
        confidence: py_round(confidence, 3),
        net_flow_ratio: None,
        gossip_signals: signals,
        has_liquidity_ads: has_lads,
    }
}

/// A channel-open candidate discovered adjacent to a known sink peer (port
/// of the dict literal `find_sink_adjacent_candidates` builds).
#[derive(Clone, Debug, PartialEq)]
pub struct SinkAdjacentCandidate {
    pub peer_id: String,
    pub source: &'static str,
    pub score: f64,
    pub reason: String,
    pub sink_peer_id: String,
    pub is_sink_adjacent: bool,
}

/// Port of `DemandFlowClassifier.find_sink_adjacent_candidates`.
///
/// `sink_profiles` is an ordered slice (not a map) precisely because ties
/// in the `abs(net_flow_ratio)` ranking key are broken by ORIGINAL
/// insertion order (Python `dict.values()` iterates in insertion order,
/// and `sorted(..., reverse=True)` is stable — ties keep that order, they
/// are NOT reversed). Callers must supply `sink_profiles` in the same
/// order the Python fixture generator's dict literal used for any case
/// meant to exercise a tie.
pub fn find_sink_adjacent_candidates(
    sink_profiles: &[NodeFlowProfile],
    sink_channels: &HashMap<String, Vec<Value>>,
    existing_peers: &HashSet<String>,
) -> Vec<SinkAdjacentCandidate> {
    if sink_profiles.is_empty() {
        return Vec::new();
    }

    // Stable descending sort by |net_flow_ratio|: comparing `b` against `a`
    // (not sorting ascending then reversing) preserves the original
    // relative order of ties, matching Python's `sorted(..., reverse=True)`
    // (see module doc comment / brief's "sort stability" note).
    let mut ranked: Vec<&NodeFlowProfile> = sink_profiles.iter().collect();
    ranked.sort_by(|a, b| {
        let ka = a.net_flow_ratio.unwrap_or(0.0).abs();
        let kb = b.net_flow_ratio.unwrap_or(0.0).abs();
        kb.partial_cmp(&ka).expect("net_flow_ratio must be finite")
    });
    ranked.truncate(5);

    let mut candidates: Vec<SinkAdjacentCandidate> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    for (rank, sink) in ranked.iter().enumerate() {
        let channels = match sink_channels.get(&sink.node_id) {
            Some(chs) => chs,
            None => continue,
        };
        for ch in channels {
            let dest = match ch.get("destination").and_then(Value::as_str) {
                Some(d) if !d.is_empty() => d,
                _ => continue,
            };
            if existing_peers.contains(dest) || seen.contains(dest) {
                continue;
            }
            if !ch.get("active").map(json_truthy).unwrap_or(false) {
                continue;
            }

            let score =
                0.4 * sink.confidence * (1.0 + (ranked.len() - rank) as f64 / ranked.len() as f64);
            let short_id: String = sink.node_id.chars().take(12).collect();
            candidates.push(SinkAdjacentCandidate {
                peer_id: dest.to_string(),
                source: "demand_flow",
                score: py_round(score, 4),
                reason: format!(
                    "Adjacent to sink {short_id}... (conf={})",
                    py_repr(sink.confidence)
                ),
                sink_peer_id: sink.node_id.clone(),
                is_sink_adjacent: true,
            });
            seen.insert(dest.to_string());
        }
    }

    candidates.sort_by(|a, b| b.score.partial_cmp(&a.score).expect("score must be finite"));
    candidates.truncate(10);
    candidates
}
