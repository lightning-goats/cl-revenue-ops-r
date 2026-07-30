//! Task 67 slice 3: the flow-analysis owner is fail-closed on every
//! required source, observation-only, and feeds the frozen kernels.

use std::collections::BTreeMap;

use revops::flow_owner::{
    refuse_retention_mutation, run_flow_pass, ChannelHistory, FlowDeps, FlowRefusal,
    REFUSED_MUTATIONS,
};
use revops_analytics::flow::{EmaBucket, HourlyHistogramBucket};
use revops_analytics::kalman::{DailyBucket, NetFlowEntry};
use serde_json::json;

const NOW: i64 = 1_800_000_000;

fn healthy_channels() -> serde_json::Value {
    json!({"channels": [
        // F71-R19: py reads SPENDABLE (net of reserve and pending
        // HTLCs), not to_us. These fixtures carry both so a regression
        // back to to_us_msat changes the derived ratios visibly.
        {"state": "CHANNELD_NORMAL", "short_channel_id": "700x1x0", "peer_id": "02aa",
         "spendable_msat": 800_000_000i64, "receivable_msat": 200_000_000i64,
         "to_us_msat": 950_000_000i64, "total_msat": 1_000_000_000i64},
        {"state": "CHANNELD_NORMAL", "short_channel_id": "800x1x0", "peer_id": "02bb",
         "spendable_msat": 100_000_000i64, "receivable_msat": 900_000_000i64,
         "to_us_msat": 250_000_000i64, "total_msat": 1_000_000_000i64},
    ]})
}

fn deps(boot: &str) -> FlowDeps<'_> {
    FlowDeps {
        peer_channels_raw: Ok(healthy_channels()),
        history: Ok(BTreeMap::new()),
        source_threshold: 0.7,
        sink_threshold: 0.3,
        flow_window_days: 7,
        htlc_congestion_threshold: 0.8,
        now: NOW,
        boot_id: boot,
    }
}

/// Every REQUIRED source refuses typed; nothing defaults.
#[test]
fn required_sources_refuse_typed() {
    let mut d = deps("boot-a");
    d.peer_channels_raw = Err("listpeerchannels rpc timeout".into());
    let err = run_flow_pass(d).expect_err("channel failure refuses");
    assert_eq!(err.code(), "flow_peer_channels_unavailable");

    // A reply with no channels array is UNUSABLE evidence, not "no
    // channels" -- the distinction that matters.
    let mut d = deps("boot-a");
    d.peer_channels_raw = Ok(json!({"result": "ok"}));
    let err = run_flow_pass(d).expect_err("shapeless reply refuses");
    assert!(matches!(err, FlowRefusal::PeerChannelsUnavailable(_)));

    let mut d = deps("boot-a");
    d.history = Err("kalman state read failed".into());
    let err = run_flow_pass(d).expect_err("history failure refuses");
    assert_eq!(err.code(), "flow_history_unavailable");
}

/// A healthy pass classifies every routable channel, stamps the boot id,
/// and returns Kalman state to persist.
#[test]
fn healthy_pass_classifies_and_stamps_provenance() {
    let result = run_flow_pass(deps("boot-a")).expect("healthy pass");
    assert_eq!(result.states.len(), 2);
    assert_eq!(result.kalman.len(), 2);
    for row in &result.states {
        assert_eq!(row.boot_id, "boot-a", "every row names the writing boot");
        assert_eq!(row.updated_at, NOW);
        assert!(!row.flow_state.is_empty());
        assert!(!row.balance_position.is_empty());
    }
    // The 80%-local channel and the 10%-local channel must not classify
    // identically -- the kernels are actually being consulted.
    let a = result.states.iter().find(|r| r.scid == "700x1x0").unwrap();
    let b = result.states.iter().find(|r| r.scid == "800x1x0").unwrap();
    assert_ne!(
        a.balance_position, b.balance_position,
        "an 80/20 and a 10/90 channel must classify differently"
    );
}

/// History feeds the kernels: a channel with forwarding history produces
/// a different ratio and a nonzero forward count.
#[test]
fn history_feeds_the_frozen_kernels() {
    let mut history = BTreeMap::new();
    history.insert(
        "700x1x0".to_string(),
        ChannelHistory {
            daily: vec![
                DailyBucket {
                    out: 500_000.0,
                    in_: 100_000.0,
                },
                DailyBucket {
                    out: 400_000.0,
                    in_: 120_000.0,
                },
            ],
            ema: vec![
                EmaBucket {
                    in_sats: 100_000,
                    out_sats: 500_000,
                    count: 12,
                    last_ts: NOW - 86_400,
                },
                EmaBucket {
                    in_sats: 120_000,
                    out_sats: 400_000,
                    count: 9,
                    last_ts: NOW - 3_600,
                },
            ],
            kalman: None,
            // F71-R19: the Kalman filter observes THESE, not the EMA
            // buckets above. Net outflow over the last 24h.
            raw_entries: vec![
                NetFlowEntry {
                    timestamp: (NOW - 3_600) as f64,
                    net_msat: 300_000_000,
                },
                NetFlowEntry {
                    timestamp: (NOW - 7_200) as f64,
                    net_msat: 200_000_000,
                },
            ],
            hourly_histogram: None,
            temporal_profile: None,
            dominant_bucket_override: None,
            posterior_variance: None,
            previous_kalman_ratio: 0.0,
            previous_state: Some("balanced".to_string()),
            previous_ratio: 0.5,
            previous_ratio_at: NOW - 3_600,
        },
    );
    let mut d = deps("boot-a");
    d.history = Ok(history);
    let result = run_flow_pass(d).expect("pass with history");
    let row = result.states.iter().find(|r| r.scid == "700x1x0").unwrap();
    assert_eq!(row.forward_count, 21, "EMA buckets' counts are summed");
    assert!(
        row.flow_ratio > 0.0,
        "the kalman kernel produced a ratio: {row:?}"
    );
    // The channel with no history still classifies (kernels are total).
    assert!(result.states.iter().any(|r| r.scid == "800x1x0"));
}

/// Unroutable and zero-capacity channels are SKIPPED WITH A REASON, not
/// silently dropped and not errors.
#[test]
fn unusable_channels_are_skipped_with_reasons() {
    let mut d = deps("boot-a");
    d.peer_channels_raw = Ok(json!({"channels": [
        // No scid: skipped BEFORE balances are required, so it needs none.
        {"peer_id": "02cc", "spendable_msat": 1i64, "total_msat": 2i64},
        // A genuinely empty channel: measured zeros, not missing fields.
        {"state": "CHANNELD_NORMAL", "short_channel_id": "900x1x0", "peer_id": "02dd",
         "spendable_msat": 0i64, "receivable_msat": 0i64, "total_msat": 0i64},
        {"state": "CHANNELD_NORMAL", "short_channel_id": "700x1x0", "peer_id": "02aa",
         "spendable_msat": 500_000_000i64, "receivable_msat": 500_000_000i64,
         "total_msat": 1_000_000_000i64},
    ]}));
    let result = run_flow_pass(d).expect("pass");
    assert_eq!(
        result.states.len(),
        1,
        "only the routable channel classifies"
    );
    assert_eq!(result.skipped.len(), 2);
    assert!(result
        .skipped
        .iter()
        .any(|(_, why)| why.contains("short_channel_id")));
    assert!(result
        .skipped
        .iter()
        .any(|(_, why)| why.contains("zero capacity")));
}

/// OBSERVATION-ONLY: Python's three flow-loop mutations are refused
/// typed, and the module names no mutation surface.
#[test]
fn python_flow_loop_mutations_are_refused() {
    assert_eq!(
        REFUSED_MUTATIONS,
        [
            "cleanup_old_data",
            "cleanup_expired_policies",
            "decay_reputation"
        ]
    );
    for operation in REFUSED_MUTATIONS {
        let refusal = refuse_retention_mutation(operation);
        assert_eq!(refusal.code(), "flow_retention_not_this_owner");
        match refusal {
            FlowRefusal::RetentionNotThisOwner { operation: named } => {
                assert_eq!(named, operation, "the refusal names the exact operation")
            }
            other => panic!("{other:?}"),
        }
    }

    // The owner cannot express a write: no store handle, no capability,
    // no delete/insert SQL anywhere in the module.
    let source =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/flow_owner.rs")).unwrap();
    for forbidden in [
        "DELETE FROM",
        "INSERT INTO",
        "UPDATE ",
        "decay_reputation(",
        "cleanup_old_data(",
        "ObserverHandle",
    ] {
        assert!(
            !source.contains(forbidden),
            "flow_owner.rs must be observation-only (found `{forbidden}`)"
        );
    }
}

// =====================================================================
// F71-R19 parity guards
// =====================================================================

/// The Kalman filter must observe the RAW 24h window, not the EMA.
///
/// Python's `_compute_raw_kalman_observation` says it "bypasses the EMA
/// pipeline to provide an unsmoothed observation that satisfies the Kalman
/// filter's measurement assumptions". The first Rust owner fed it
/// `ema_out / (ema_in + ema_out)` instead. This pins the difference: heavy
/// EMA history with an EMPTY raw window must leave the filter unmoved.
#[test]
fn ema_history_alone_does_not_move_the_kalman_estimate() {
    let mut history = BTreeMap::new();
    history.insert(
        "700x1x0".to_string(),
        ChannelHistory {
            daily: vec![
                DailyBucket {
                    out: 900_000.0,
                    in_: 10_000.0,
                },
                DailyBucket {
                    out: 850_000.0,
                    in_: 20_000.0,
                },
                DailyBucket {
                    out: 800_000.0,
                    in_: 15_000.0,
                },
            ],
            ema: vec![EmaBucket {
                in_sats: 10_000,
                out_sats: 900_000,
                count: 40,
                last_ts: NOW - 3_600,
            }],
            kalman: None,
            // The whole point: no raw observation in the last 24h.
            raw_entries: Vec::new(),
            hourly_histogram: None,
            temporal_profile: None,
            dominant_bucket_override: None,
            posterior_variance: None,
            previous_kalman_ratio: 0.0,
            previous_state: None,
            previous_ratio: 0.0,
            previous_ratio_at: 0,
        },
    );
    let mut d = deps("boot-a");
    d.history = Ok(history);
    let result = run_flow_pass(d).expect("pass with EMA-only history");
    let row = result.states.iter().find(|r| r.scid == "700x1x0").unwrap();
    assert_eq!(
        row.kalman_flow_ratio, 0.0,
        "a lopsided EMA with no raw 24h observation must NOT drive the \
         Kalman estimate; it did under the pre-R19 owner"
    );
    // F71-R20: and the EMA ratio is still recorded, in its OWN column.
    // If these two ever collapse into one field again, this pair fails.
    assert!(
        row.flow_ratio > 0.5,
        "the EMA ratio is a separate, real observation: {row:?}"
    );
}

/// The balance ratio comes from `spendable_msat`, not `to_us_msat`.
///
/// CLN's `spendable_msat` already nets out the channel reserve and pending
/// HTLCs. `to_us_msat` counts sats we cannot actually send, so a channel
/// pinned by its reserve reads as well-funded while it can route nothing.
/// The fixture is built so the two fields disagree across the classifier's
/// balance boundary.
#[test]
fn balance_position_reads_spendable_not_to_us() {
    let reserve_pinned = json!({"channels": [
        // to_us says 95% ours; spendable says 5% — only 50_000_000 msat
        // is actually sendable, the rest is reserve and in-flight HTLCs.
        {"state": "CHANNELD_NORMAL", "short_channel_id": "900x1x0", "peer_id": "02cc",
         "to_us_msat": 950_000_000i64, "spendable_msat": 50_000_000i64,
         "receivable_msat": 950_000_000i64, "total_msat": 1_000_000_000i64},
    ]});
    let mut d = deps("boot-a");
    d.peer_channels_raw = Ok(reserve_pinned);
    let result = run_flow_pass(d).expect("pass over a reserve-pinned channel");
    let row = &result.states[0];
    assert_ne!(
        row.balance_position, "high_local",
        "reading to_us would call this channel richly funded outbound when \
         it can send almost nothing: {row:?}"
    );
}

/// A channel with no `total_msat` falls back to spendable+receivable,
/// exactly as py does — it is not skipped as zero-capacity.
#[test]
fn absent_total_msat_falls_back_to_spendable_plus_receivable() {
    let no_total = json!({"channels": [
        {"state": "CHANNELD_NORMAL", "short_channel_id": "910x1x0", "peer_id": "02dd",
         "spendable_msat": 400_000_000i64, "receivable_msat": 600_000_000i64},
    ]});
    let mut d = deps("boot-a");
    d.peer_channels_raw = Ok(no_total);
    let result = run_flow_pass(d).expect("pass with no total_msat");
    assert_eq!(
        result.states.len(),
        1,
        "capacity must fall back, not collapse to zero and skip: {:?}",
        result.skipped
    );
}

/// A PRESENT but corrupt money field refuses. An ABSENT one is py's own
/// `.get(field, 0) or 0` default and stays a measured zero.
#[test]
fn corrupt_money_fields_refuse_but_absent_ones_default() {
    let corrupt = json!({"channels": [
        {"state": "CHANNELD_NORMAL", "short_channel_id": "920x1x0", "peer_id": "02ee",
         "spendable_msat": "not-a-number", "receivable_msat": 600_000_000i64,
         "total_msat": 1_000_000_000i64},
    ]});
    let mut d = deps("boot-a");
    d.peer_channels_raw = Ok(corrupt);
    let err = run_flow_pass(d).expect_err("a corrupt spendable must refuse");
    assert_eq!(err.code(), "flow_peer_channels_malformed");

    // F71-R20: an ABSENT required balance refuses too. Defaulting it to 0
    // is not a harmless blank -- it is the confident claim "this channel
    // can send nothing", which drives the balance classifier and every
    // fee/rebalance surface downstream.
    let absent = json!({"channels": [
        {"state": "CHANNELD_NORMAL", "short_channel_id": "930x1x0", "peer_id": "02ff",
         "receivable_msat": 600_000_000i64, "total_msat": 1_000_000_000i64},
    ]});
    let mut d = deps("boot-a");
    d.peer_channels_raw = Ok(absent);
    let err = run_flow_pass(d).expect_err("a missing spendable must refuse, not read as zero");
    assert_eq!(err.code(), "flow_peer_channels_malformed");
}

/// HTLC-slot saturation classifies CONGESTED and, per py, suppresses the
/// Kalman state override entirely.
#[test]
fn htlc_saturation_classifies_congested() {
    let congested = json!({"channels": [
        {"state": "CHANNELD_NORMAL", "short_channel_id": "940x1x0", "peer_id": "02ab",
         "spendable_msat": 500_000_000i64, "receivable_msat": 500_000_000i64,
         "total_msat": 1_000_000_000i64,
         "max_accepted_htlcs": 10,
         "htlcs": [{}, {}, {}, {}, {}, {}, {}, {}, {}]},
    ]});
    let mut d = deps("boot-a");
    d.peer_channels_raw = Ok(congested);
    let result = run_flow_pass(d).expect("pass over a congested channel");
    assert_eq!(result.states[0].flow_state, "congested");
}

// =====================================================================
// F71-R20 parity guards
// =====================================================================

/// py `channel.get("max_htlcs", 483)`. With a 0 default, a channel that
/// reports active HTLCs but no limit divides by zero, reads as 0.0
/// utilization, and can never be CONGESTED — while py computes a real
/// utilization against the protocol ceiling.
#[test]
fn absent_max_htlcs_defaults_to_the_protocol_ceiling_not_zero() {
    let saturated = json!({"channels": [
        // 400 active HTLCs, no reported limit. Against py's 483 default
        // that is 82.8% utilization -- above the 0.8 threshold.
        {"state": "CHANNELD_NORMAL", "short_channel_id": "950x1x0", "peer_id": "02ba",
         "spendable_msat": 500_000_000i64, "receivable_msat": 500_000_000i64,
         "total_msat": 1_000_000_000i64, "active_htlcs": 400},
    ]});
    let mut d = deps("boot-a");
    d.peer_channels_raw = Ok(saturated);
    let result = run_flow_pass(d).expect("pass over a saturated channel");
    assert_eq!(
        result.states[0].flow_state, "congested",
        "a 0 default for max_htlcs would silently make this uncongested"
    );
}

/// The DTS widening must actually fire. py reads each channel's
/// `posterior_variance` and, when the fee controller is still exploring
/// (> 10000), widens the flow thresholds by 50% to bias toward BALANCED.
/// Hardcoding `None` suppressed every widening.
#[test]
fn exploring_dts_variance_widens_the_flow_thresholds() {
    // Raw 24h net outflow of ~6% of capacity: above a 0.05 source
    // threshold, but below the 0.075 that a 1.5x widening produces.
    let raw = vec![NetFlowEntry {
        timestamp: (NOW - 600) as f64,
        net_msat: 60_000_000,
    }];
    let converged = revops_analytics::kalman::KalmanFlowState {
        flow_ratio: 0.06,
        variance_ratio: 0.001,
        variance_velocity: 0.001,
        observation_count: 50,
        last_update: NOW - 7_200,
        ..Default::default()
    };

    let classify = |variance: Option<f64>| {
        let mut history = BTreeMap::new();
        history.insert(
            "700x1x0".to_string(),
            ChannelHistory {
                daily: Vec::new(),
                ema: Vec::new(),
                kalman: Some(converged.clone()),
                raw_entries: raw.clone(),
                hourly_histogram: None,
                temporal_profile: None,
                dominant_bucket_override: None,
                posterior_variance: variance,
                previous_kalman_ratio: 0.0,
                previous_state: None,
                previous_ratio: 0.0,
                previous_ratio_at: 0,
            },
        );
        let mut d = deps("boot-a");
        d.source_threshold = 0.05;
        d.sink_threshold = -0.05;
        d.history = Ok(history);
        let result = run_flow_pass(d).expect("pass");
        result
            .states
            .iter()
            .find(|r| r.scid == "700x1x0")
            .unwrap()
            .flow_state
            .clone()
    };

    let settled = classify(Some(5_000.0));
    let exploring = classify(Some(50_000.0));
    assert_ne!(
        settled, exploring,
        "an exploring DTS controller must widen the thresholds and change \
         the classification; hardcoding None made these identical"
    );
}

// =====================================================================
// F71-R21 parity guards
// =====================================================================

/// py analyses ONLY `CHANNELD_NORMAL` channels (flow_analysis.py:2119).
/// A channel that is opening, closing, or awaiting lock-in has no
/// meaningful steady-state flow; classifying one persists a state row py
/// never writes, which the fee and rebalance surfaces would then act on.
#[test]
fn only_channeld_normal_channels_are_analysed() {
    let mixed = json!({"channels": [
        {"state": "CHANNELD_NORMAL", "short_channel_id": "960x1x0", "peer_id": "02a1",
         "spendable_msat": 500_000_000i64, "receivable_msat": 500_000_000i64,
         "total_msat": 1_000_000_000i64},
        {"state": "CHANNELD_AWAITING_LOCKIN", "short_channel_id": "961x1x0", "peer_id": "02a2",
         "spendable_msat": 500_000_000i64, "receivable_msat": 500_000_000i64,
         "total_msat": 1_000_000_000i64},
        {"state": "CLOSINGD_SIGEXCHANGE", "short_channel_id": "962x1x0", "peer_id": "02a3",
         "spendable_msat": 500_000_000i64, "receivable_msat": 500_000_000i64,
         "total_msat": 1_000_000_000i64},
    ]});
    let mut d = deps("boot-a");
    d.peer_channels_raw = Ok(mixed);
    let result = run_flow_pass(d).expect("pass over a mixed-state fleet");
    assert_eq!(
        result.states.len(),
        1,
        "only the CHANNELD_NORMAL channel classifies, got {:?}",
        result.states.iter().map(|s| &s.scid).collect::<Vec<_>>()
    );
    assert_eq!(result.states[0].scid, "960x1x0");
    assert_eq!(
        result.skipped.len(),
        2,
        "the other two are skipped WITH A REASON, not silently dropped"
    );
    assert!(result
        .skipped
        .iter()
        .all(|(_, why)| why.contains("CHANNELD_NORMAL")));
}

/// The balance classifier's veto reads the PREVIOUS cycle's persisted
/// Kalman estimate (py `prev_state["kalman_flow_ratio"]`,
/// flow_analysis.py:1468-1471 -> :1963). Feeding it the fresh estimate
/// instead makes the veto circular — the filter would be vetoing on its
/// own current output.
#[test]
fn balance_veto_reads_the_previous_cycle_kalman_ratio() {
    // No flow data at all, so the classifier takes py's balance fallback,
    // where the veto input is the only thing that varies.
    let channels = json!({"channels": [
        {"state": "CHANNELD_NORMAL", "short_channel_id": "970x1x0", "peer_id": "02b1",
         "spendable_msat": 780_000_000i64, "receivable_msat": 220_000_000i64,
         "total_msat": 1_000_000_000i64},
    ]});

    let classify = |previous_kalman_ratio: f64| {
        let mut history = BTreeMap::new();
        history.insert(
            "970x1x0".to_string(),
            ChannelHistory {
                daily: Vec::new(),
                ema: Vec::new(),
                kalman: None,
                raw_entries: Vec::new(),
                hourly_histogram: None,
                temporal_profile: None,
                dominant_bucket_override: None,
                posterior_variance: None,
                previous_state: Some("source".to_string()),
                previous_kalman_ratio,
                previous_ratio: 0.0,
                previous_ratio_at: 0,
            },
        );
        let mut d = deps("boot-a");
        d.peer_channels_raw = Ok(channels.clone());
        d.history = Ok(history);
        let result = run_flow_pass(d).expect("pass");
        result.states[0].flow_state.clone()
    };

    assert_ne!(
        classify(0.0),
        classify(-0.9),
        "a strongly negative PREVIOUS Kalman estimate must veto the \
         balance-derived SOURCE label; if these agree the veto input is \
         being ignored"
    );
}

/// F71-R21a. In py's HAS-FLOW branch the threshold test and the veto read
/// two DIFFERENT signals: it thresholds on the EMA ratio, but the
/// balanced-zone fallback vetoes with the PREVIOUS Kalman estimate
/// (flow_analysis.py:1948-1963).
///
/// `classification::flow_state` cannot express that split — it forwards
/// the value it thresholded on as the veto argument. That is right for the
/// Kalman reclassification path, where both genuinely are the fresh kalman
/// ratio, and wrong here. Borrowing it for the EMA branch silently fed the
/// EMA ratio in as the veto.
#[test]
fn has_flow_balanced_zone_vetoes_with_previous_kalman_not_the_ema_ratio() {
    let channels = json!({"channels": [
        {"state": "CHANNELD_NORMAL", "short_channel_id": "980x1x0", "peer_id": "02c1",
         "spendable_msat": 800_000_000i64, "receivable_msat": 200_000_000i64,
         "total_msat": 1_000_000_000i64},
    ]});

    // Real forward activity, but a NET flow well inside the balanced band,
    // so py falls through to the balance-position veto.
    let ema = vec![EmaBucket {
        in_sats: 100_000,
        out_sats: 101_000,
        count: 30,
        last_ts: NOW - 3_600,
    }];

    let classify = |previous_kalman_ratio: f64| {
        let mut history = BTreeMap::new();
        history.insert(
            "980x1x0".to_string(),
            ChannelHistory {
                daily: Vec::new(),
                ema: ema.clone(),
                kalman: None,
                raw_entries: Vec::new(),
                hourly_histogram: None,
                temporal_profile: None,
                dominant_bucket_override: None,
                posterior_variance: None,
                previous_state: Some("source".to_string()),
                previous_kalman_ratio,
                previous_ratio: 0.0,
                previous_ratio_at: 0,
            },
        );
        let mut d = deps("boot-a");
        d.source_threshold = 0.05;
        d.sink_threshold = -0.05;
        d.peer_channels_raw = Ok(channels.clone());
        d.history = Ok(history);
        let result = run_flow_pass(d).expect("pass");
        result.states[0].flow_state.clone()
    };

    // outbound 0.8 > SINK_ENTER (0.78) makes this a SINK candidate; only a
    // POSITIVE previous-Kalman ratio above KALMAN_BALANCE_VETO_RATIO
    // (0.05) vetoes that label. The EMA ratio here is ~0.001, far below
    // the veto band, so if it were being used as the veto both calls would
    // agree.
    assert_eq!(classify(0.0), "sink", "no veto: the balance signal stands");
    assert_ne!(
        classify(0.0),
        classify(0.9),
        "the balanced-zone veto must read the PREVIOUS Kalman estimate; if \
         these agree the EMA ratio is being used as its own veto"
    );
}

/// F71-R24: the retention set is every scid the snapshot carried, in ANY
/// state — including the transient-state channels R21 declines to analyse.
/// Reconciling against the ANALYSED set would purge their accumulated
/// Kalman state.
#[test]
fn observed_scids_include_channels_that_were_skipped_not_analysed() {
    let mixed = json!({"channels": [
        {"state": "CHANNELD_NORMAL", "short_channel_id": "990x1x0", "peer_id": "02d1",
         "spendable_msat": 500_000_000i64, "receivable_msat": 500_000_000i64,
         "total_msat": 1_000_000_000i64},
        {"state": "CHANNELD_AWAITING_LOCKIN", "short_channel_id": "991x1x0", "peer_id": "02d2",
         "spendable_msat": 500_000_000i64, "receivable_msat": 500_000_000i64,
         "total_msat": 1_000_000_000i64},
        // Zero-capacity: skipped for a different reason, still EXISTS.
        {"state": "CHANNELD_NORMAL", "short_channel_id": "992x1x0", "peer_id": "02d3",
         "spendable_msat": 0i64, "receivable_msat": 0i64, "total_msat": 0i64},
        // No scid at all: cannot be retained, has nothing to key on.
        {"state": "CHANNELD_NORMAL", "peer_id": "02d4",
         "spendable_msat": 1i64, "receivable_msat": 1i64, "total_msat": 2i64},
    ]});
    let mut d = deps("boot-a");
    d.peer_channels_raw = Ok(mixed);
    let result = run_flow_pass(d).expect("pass");

    assert_eq!(result.states.len(), 1, "only one channel is ANALYSED");
    let observed: Vec<&str> = result.observed_scids.iter().map(String::as_str).collect();
    assert_eq!(
        observed,
        vec!["990x1x0", "991x1x0", "992x1x0"],
        "every scid present in the snapshot is retained, analysed or not"
    );
}

/// F71-R25: the temporal kernel actually runs and its output is carried
/// for the atomic commit. Before this, both the store and the frozen
/// kernel existed and nothing connected them.
#[test]
fn the_temporal_profile_is_produced_from_the_hourly_histogram() {
    let mut histogram = [HourlyHistogramBucket::default(); 24];
    // A clearly diurnal channel: all its flow in one hour.
    histogram[9] = HourlyHistogramBucket {
        out_sats: 900_000.0,
        in_sats: 10_000.0,
        count: 40.0,
    };
    let mut history = BTreeMap::new();
    history.insert(
        "700x1x0".to_string(),
        ChannelHistory {
            daily: Vec::new(),
            ema: Vec::new(),
            kalman: None,
            raw_entries: Vec::new(),
            hourly_histogram: Some(histogram),
            temporal_profile: None,
            dominant_bucket_override: None,
            posterior_variance: None,
            previous_kalman_ratio: 0.0,
            previous_state: None,
            previous_ratio: 0.0,
            previous_ratio_at: 0,
        },
    );
    let mut d = deps("boot-a");
    d.history = Ok(history);
    let result = run_flow_pass(d).expect("pass");

    let (scid, profile) = result
        .temporal
        .iter()
        .find(|(s, _)| s == "700x1x0")
        .expect("the temporal kernel must run for an analysed channel");
    assert_eq!(scid, "700x1x0");
    assert_eq!(
        profile.hourly_out[9], 900_000.0,
        "a first observation is taken verbatim, not EMA-blended against zero"
    );
    assert!(
        profile.peak_hours.contains(&9),
        "the kernel recomputed its derived fields: {profile:?}"
    );
    // Every analysed channel gets a profile, so the commit stays aligned.
    assert_eq!(result.temporal.len(), result.states.len());
}

/// F71-R25b: the temporal profile's `dominant_bucket` is owned by the FEE
/// controller's size profiling. py sets it on the existing profile before
/// calling the kernel, which carries it forward unchanged.
#[test]
fn the_fee_controllers_dominant_bucket_reaches_the_temporal_profile() {
    let build = |dominant_bucket_override: Option<String>| {
        let mut history = BTreeMap::new();
        history.insert(
            "700x1x0".to_string(),
            ChannelHistory {
                daily: Vec::new(),
                ema: Vec::new(),
                kalman: None,
                raw_entries: Vec::new(),
                hourly_histogram: Some([HourlyHistogramBucket::default(); 24]),
                temporal_profile: Some(revops_analytics::flow::TemporalProfile {
                    dominant_bucket: "stored-label".to_string(),
                    ..Default::default()
                }),
                dominant_bucket_override,
                posterior_variance: None,
                previous_kalman_ratio: 0.0,
                previous_state: None,
                previous_ratio: 0.0,
                previous_ratio_at: 0,
            },
        );
        let mut d = deps("boot-a");
        d.history = Ok(history);
        let result = run_flow_pass(d).expect("pass");
        result
            .temporal
            .iter()
            .find(|(s, _)| s == "700x1x0")
            .unwrap()
            .1
            .dominant_bucket
            .clone()
    };

    assert_eq!(
        build(Some("large".to_string())),
        "large",
        "the fee controller's label must reach the persisted profile"
    );
    // py's `except: pass` path: size profiling unavailable keeps whatever
    // the stored profile already had, rather than blanking it.
    assert_eq!(
        build(None),
        "stored-label",
        "an absent override must NOT overwrite the stored label"
    );
}
