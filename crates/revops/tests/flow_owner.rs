//! Task 67 slice 3: the flow-analysis owner is fail-closed on every
//! required source, observation-only, and feeds the frozen kernels.

use std::collections::BTreeMap;

use revops::flow_owner::{
    refuse_retention_mutation, run_flow_pass, ChannelHistory, FlowDeps, FlowRefusal,
    REFUSED_MUTATIONS,
};
use revops_analytics::flow::EmaBucket;
use revops_analytics::kalman::DailyBucket;
use serde_json::json;

const NOW: i64 = 1_800_000_000;

fn healthy_channels() -> serde_json::Value {
    json!({"channels": [
        {"short_channel_id": "700x1x0", "peer_id": "02aa",
         "to_us_msat": 800_000_000i64, "total_msat": 1_000_000_000i64},
        {"short_channel_id": "800x1x0", "peer_id": "02bb",
         "to_us_msat": 100_000_000i64, "total_msat": 1_000_000_000i64},
    ]})
}

fn deps(boot: &str) -> FlowDeps<'_> {
    FlowDeps {
        peer_channels_raw: Ok(healthy_channels()),
        history: Ok(BTreeMap::new()),
        source_threshold: 0.7,
        sink_threshold: 0.3,
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
        {"peer_id": "02cc", "to_us_msat": 1i64, "total_msat": 2i64},
        {"short_channel_id": "900x1x0", "peer_id": "02dd",
         "to_us_msat": 0i64, "total_msat": 0i64},
        {"short_channel_id": "700x1x0", "peer_id": "02aa",
         "to_us_msat": 500_000_000i64, "total_msat": 1_000_000_000i64},
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
