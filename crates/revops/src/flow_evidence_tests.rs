//! F71-R23 slice 1: the current-boot gate in front of `revenue-r-analyze`.
//!
//! Every test here exists to keep one pair of outcomes apart. The port's
//! recurring failure shape is not a wrong number; it is two DIFFERENT node
//! states rendering as the same JSON, so a caller cannot tell "we looked
//! and there is nothing" from "we never looked".

use revops_db::analytics::ChannelFlowStateRow;
use revops_db::loop_health::{
    BootStatus, LoopHealthRow, LoopId, RuntimeStatus, TerminalStatus, WiringStatus,
};
use serde_json::{json, Value};

use crate::flow_evidence::{classify_flow_evidence, FlowEvidence, FlowEvidenceRefusal};
use crate::rpc_analyze::build_analyze_from_evidence;

const THIS_BOOT: &str = "boot-current";
const PRIOR_BOOT: &str = "boot-previous";

/// A flow-analysis loop row that is wired, active, and carries whatever
/// terminal the caller asks for.
fn loop_row(
    terminal_status: TerminalStatus,
    terminal_boot: Option<&str>,
    generation: u64,
    terminal_generation: u64,
    started_boot: Option<&str>,
) -> LoopHealthRow {
    let mut row = LoopHealthRow::new(LoopId::FlowAnalysis, WiringStatus::Ready, 1_000);
    row.generation = generation;
    row.terminal_generation = terminal_generation;
    row.terminal_status = terminal_status;
    row.terminal_boot_id = terminal_boot.map(str::to_string);
    row.boot_id = started_boot.map(str::to_string);
    row
}

/// The normal healthy case: this boot began and finished generation 1.
fn passed_this_boot() -> LoopHealthRow {
    loop_row(
        TerminalStatus::Passed,
        Some(THIS_BOOT),
        1,
        1,
        Some(THIS_BOOT),
    )
}

fn state_row(scid: &str, boot_id: &str) -> ChannelFlowStateRow {
    ChannelFlowStateRow {
        scid: scid.to_string(),
        peer_id: "02aa".to_string(),
        flow_state: "BALANCED".to_string(),
        balance_position: "balanced".to_string(),
        flow_ratio: 0.5,
        velocity: 1.25,
        confidence: 0.8,
        kalman_flow_ratio: 0.52,
        kalman_velocity: 1.3,
        kalman_uncertainty: 0.11,
        kalman_regime_change: false,
        forward_count: 42,
        updated_at: 1_700_000_000,
        boot_id: boot_id.to_string(),
    }
}

// ---------------------------------------------------------------------
// The decision itself.
// ---------------------------------------------------------------------

#[test]
fn a_current_boot_row_after_a_current_boot_pass_is_evidence() {
    let evidence = classify_flow_evidence(
        Some(&passed_this_boot()),
        Some(state_row("123x1x0", THIS_BOOT)),
        THIS_BOOT,
    )
    .expect("a channel this boot analysed is evidence");
    match evidence {
        FlowEvidence::Current(row) => assert_eq!(row.scid, "123x1x0"),
        FlowEvidence::NoSuchChannel => panic!("row was present and current-boot"),
    }
}

#[test]
fn a_missing_row_after_a_current_boot_pass_is_pythons_own_null_not_a_refusal() {
    // The pass RAN and did not produce this channel. That is Python's
    // legitimate `{"channel": ..., "analysis": null}` -- a real answer.
    // Refusing here would turn every unknown SCID into an error Python
    // never returns.
    let evidence = classify_flow_evidence(Some(&passed_this_boot()), None, THIS_BOOT)
        .expect("a completed pass with no row for this channel is a real answer");
    assert_eq!(evidence, FlowEvidence::NoSuchChannel);
}

#[test]
fn no_pass_this_boot_refuses_rather_than_reporting_no_data() {
    // THE case this slice exists for. Between process start and the flow
    // loop's first pass (30s, F71-R26) the store still holds every row the
    // PREVIOUS boot wrote. Serving them is stale evidence; serving `null`
    // instead is a false "this channel has no flow data" for the entire
    // fleet at once. Only a refusal is true.
    let row = loop_row(
        TerminalStatus::Passed,
        Some(PRIOR_BOOT),
        7,
        7,
        Some(PRIOR_BOOT),
    );
    let refusal = classify_flow_evidence(
        Some(&row),
        Some(state_row("123x1x0", PRIOR_BOOT)),
        THIS_BOOT,
    )
    .expect_err("a prior boot's terminal is history, never inherited");
    assert_eq!(
        refusal,
        FlowEvidenceRefusal::NoPassThisBoot(BootStatus::NeverRunThisBoot)
    );
}

#[test]
fn an_in_flight_first_pass_is_not_yet_evidence() {
    // This boot STARTED generation 1 and has not reached a terminal. The
    // store holds a partially-written pass: some channels updated, the
    // rest still carrying the prior boot's numbers. Half a pass is not a
    // pass.
    let row = loop_row(TerminalStatus::None, None, 1, 0, Some(THIS_BOOT));
    let refusal =
        classify_flow_evidence(Some(&row), Some(state_row("123x1x0", THIS_BOOT)), THIS_BOOT)
            .expect_err("an incomplete pass is not evidence");
    assert_eq!(
        refusal,
        FlowEvidenceRefusal::NoPassThisBoot(BootStatus::Incomplete)
    );
}

#[test]
fn a_failed_pass_this_boot_is_not_evidence() {
    let row = loop_row(
        TerminalStatus::Error,
        Some(THIS_BOOT),
        1,
        1,
        Some(THIS_BOOT),
    );
    let refusal =
        classify_flow_evidence(Some(&row), Some(state_row("123x1x0", THIS_BOOT)), THIS_BOOT)
            .expect_err("an errored pass is not evidence");
    assert_eq!(
        refusal,
        FlowEvidenceRefusal::NoPassThisBoot(BootStatus::Error)
    );
}

#[test]
fn a_suspended_loop_is_not_evidence() {
    let mut row = passed_this_boot();
    row.runtime_status = RuntimeStatus::Suspended;
    let refusal =
        classify_flow_evidence(Some(&row), Some(state_row("123x1x0", THIS_BOOT)), THIS_BOOT)
            .expect_err("a suspended loop stops producing evidence");
    assert_eq!(
        refusal,
        FlowEvidenceRefusal::NoPassThisBoot(BootStatus::Suspended)
    );
}

#[test]
fn an_unregistered_loop_is_distinct_from_a_loop_that_has_not_run() {
    // No row at all means the loop was never registered in this process --
    // a wiring defect. Collapsing it into `NeverRunThisBoot` would make a
    // permanently-dead surface look like one that is merely warming up.
    let refusal = classify_flow_evidence(None, None, THIS_BOOT)
        .expect_err("an unregistered loop cannot have produced evidence");
    assert_eq!(refusal, FlowEvidenceRefusal::LoopUnregistered);
}

#[test]
fn a_prior_boot_row_is_refused_even_after_a_current_boot_pass() {
    // The pass completed, but THIS channel's row was written by an earlier
    // process -- the pass did not cover it (the live snapshot still listed
    // the channel, so closed-channel reconciliation did not remove it,
    // F71-R21a). Its numbers describe a different process's view of the
    // node. `updated_at` alone cannot catch this: a prior boot minutes ago
    // looks fresher than a current boot's first pass.
    let refusal = classify_flow_evidence(
        Some(&passed_this_boot()),
        Some(state_row("123x1x0", PRIOR_BOOT)),
        THIS_BOOT,
    )
    .expect_err("a prior boot's row is not this boot's evidence");
    assert_eq!(
        refusal,
        FlowEvidenceRefusal::StalePriorBootRow {
            scid: "123x1x0".to_string(),
            row_boot_id: PRIOR_BOOT.to_string(),
        }
    );
}

#[test]
fn every_refusal_carries_a_distinct_stable_code() {
    let codes = [
        FlowEvidenceRefusal::StoreNotConfigured.code(),
        FlowEvidenceRefusal::LoopUnregistered.code(),
        FlowEvidenceRefusal::NoPassThisBoot(BootStatus::NeverRunThisBoot).code(),
        FlowEvidenceRefusal::StalePriorBootRow {
            scid: "x".to_string(),
            row_boot_id: "y".to_string(),
        }
        .code(),
        FlowEvidenceRefusal::StoreUnavailable("io".to_string()).code(),
    ];
    let mut unique: Vec<&str> = codes.to_vec();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(
        unique.len(),
        codes.len(),
        "two refusals sharing a code cannot be told apart by a caller: {codes:?}"
    );
    for code in codes {
        assert_ne!(
            code, "not_yet_ported",
            "a live refusal must not reuse the marker that means 'this port never implemented it'"
        );
    }
}

// ---------------------------------------------------------------------
// The response projection.
// ---------------------------------------------------------------------

#[test]
fn a_refusal_response_omits_the_analysis_key_entirely() {
    // Python's own unknown-channel answer is `{"channel": ..., "analysis":
    // null}`. If a refusal ALSO carried `analysis: null`, a caller reading
    // `result["analysis"]` -- which is what the field is for -- would read
    // a refusal as a real "no data" answer and never look at `error`. The
    // key is absent so that read fails loudly instead.
    let refusal = FlowEvidenceRefusal::NoPassThisBoot(BootStatus::NeverRunThisBoot);
    let v = build_analyze_from_evidence(Some(&json!("123x1x0")), Err(&refusal));
    assert!(
        v.get("analysis").is_none(),
        "refusal must not present an `analysis` key at all, got {v}"
    );
    assert_eq!(v["error"], "flow_evidence_no_pass_this_boot");
    assert_eq!(v["channel"], "123x1x0");
    assert_eq!(
        v["boot_status"], "never_run_this_boot",
        "the operator needs to know WHICH not-ready state this is"
    );
}

#[test]
fn a_no_such_channel_response_is_byte_identical_to_pythons_null_answer() {
    let v = build_analyze_from_evidence(Some(&json!("123x1x0")), Ok(&FlowEvidence::NoSuchChannel));
    assert_eq!(v, json!({"channel": "123x1x0", "analysis": Value::Null}));
}

#[test]
fn a_served_row_reports_the_boot_that_produced_it() {
    let evidence = FlowEvidence::Current(state_row("123x1x0", THIS_BOOT));
    let v = build_analyze_from_evidence(Some(&json!("123x1x0")), Ok(&evidence));
    assert_eq!(v["channel"], "123x1x0");
    assert_eq!(v["analysis"]["boot_id"], THIS_BOOT);
    assert_eq!(v["analysis"]["flow_ratio"], 0.5);
    assert_eq!(v["analysis"]["kalman_flow_ratio"], 0.52);
    assert!(
        v.get("error").is_none(),
        "a served row is not an error, got {v}"
    );
}

/// main.rs is a binary no test can import -- the same blind spot that let
/// R16's composition defect survive. A source-level proof that `analyze`
/// is actually served from current-boot evidence, and that the marker it
/// replaced is gone.
#[test]
fn main_serves_analyze_from_current_boot_evidence() {
    let source =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/main.rs")).unwrap();

    for call in [
        "flow_evidence::current_boot_flow_evidence(",
        "rpc_analyze::analyze_target_scid(",
        "build_analyze_from_evidence(",
        "s.observer_db.as_ref()",
        "&s.boot_id",
    ] {
        assert!(
            source.contains(call),
            "main.rs must serve analyze from current-boot flow evidence (missing `{call}`)"
        );
    }
    assert!(
        !source.contains("MetricsLookup::NotWired"),
        "the not-wired marker must be gone: the store holds real rows, so claiming this \
         port never looked is now a false statement about the port"
    );
}

#[test]
fn only_a_well_formed_scid_reaches_the_store() {
    use crate::rpc_analyze::analyze_target_scid;

    assert_eq!(
        analyze_target_scid(Some(&json!("123x456x789"))),
        Some("123x456x789".to_string())
    );
    // Python's `:` spelling normalizes to the store's `x` spelling, so a
    // caller using either form reads the same row the flow pass wrote.
    assert_eq!(
        analyze_target_scid(Some(&json!("123:456:789"))),
        Some("123x456x789".to_string())
    );
    for nothing_to_look_up in [json!("not-an-scid"), json!(""), json!(7), Value::Null] {
        assert_eq!(
            analyze_target_scid(Some(&nothing_to_look_up)),
            None,
            "{nothing_to_look_up} must not reach the store"
        );
    }
    assert_eq!(analyze_target_scid(None), None);
}

/// C71-14, structural. The deterministic tear test proves the ATOMIC
/// COMMAND is consistent; it cannot prove the producer calls it, because
/// there is no seam to inject a concurrent pass into between two awaits
/// that a torn producer would have. So the producer's read shape is pinned
/// from its source instead -- the same technique the R26 composition proof
/// uses for code no test can otherwise observe.
///
/// Reverting `current_boot_flow_evidence` to a loop-health read followed
/// by a row read fails here.
#[test]
fn the_producer_reads_the_store_exactly_once() {
    let source =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/flow_evidence.rs"))
            .unwrap();
    let body = source
        .split_once("pub async fn current_boot_flow_evidence")
        .expect("the producer must exist")
        .1;

    assert_eq!(
        body.matches(".await").count(),
        1,
        "the producer must await the store exactly once; two awaits let a flow pass \
         land between them and produce a pair that was never simultaneously true"
    );
    assert!(
        body.contains("flow_evidence_snapshot("),
        "the producer must read through the atomic snapshot command"
    );
    for torn in ["list_loop_health(", "channel_flow_states("] {
        assert!(
            !body.contains(torn),
            "the producer must not reassemble the observation from parts (found `{torn}`)"
        );
    }
}

#[test]
fn a_malformed_channel_id_is_rejected_before_any_evidence_is_consulted() {
    // The parameter verdict must not depend on loop health: a caller
    // passing a non-SCID gets Python's format error whether or not the
    // flow loop has run.
    let refusal = FlowEvidenceRefusal::NoPassThisBoot(BootStatus::NeverRunThisBoot);
    let v = build_analyze_from_evidence(Some(&json!("not-an-scid")), Err(&refusal));
    assert!(
        v["error"]
            .as_str()
            .is_some_and(|e| e.starts_with("Invalid channel format")),
        "expected Python's own format error, got {v}"
    );
}
