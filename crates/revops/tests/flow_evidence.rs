//! F71-R23 slice 1, store-backed: the current-boot flow-evidence gate
//! against a real observer actor.
//!
//! The unit tests in `revops::flow_evidence_tests` pin the DECISION. These
//! pin the READ -- specifically that the two facts the decision consumes
//! arrive as one observation (C71-14).

use revops::flow_evidence::{current_boot_flow_evidence, FlowEvidence, FlowEvidenceRefusal};
use revops_db::analytics::ChannelFlowStateRow;
use revops_db::loop_health::{BootStatus, LoopId, WiringStatus};
use revops_db::owner::ObserverHandle;

const THIS_BOOT: &str = "boot-current";
const PRIOR_BOOT: &str = "boot-previous";
const SCID: &str = "700x1x0";

fn row(scid: &str, boot_id: &str, forward_count: i64) -> ChannelFlowStateRow {
    ChannelFlowStateRow {
        scid: scid.to_string(),
        peer_id: "02aa".to_string(),
        flow_state: "BALANCED".to_string(),
        balance_position: "balanced".to_string(),
        flow_ratio: 0.5,
        velocity: 1.0,
        confidence: 0.9,
        kalman_flow_ratio: 0.5,
        kalman_velocity: 1.0,
        kalman_uncertainty: 0.1,
        kalman_regime_change: false,
        forward_count,
        updated_at: 1_800_000_000,
        boot_id: boot_id.to_string(),
    }
}

async fn observer(dir: &tempfile::TempDir) -> ObserverHandle {
    let handle = revops_db::owner::spawn_read_write(&dir.path().join("obs.db"))
        .await
        .expect("observer store");
    handle
        .register_loop(LoopId::FlowAnalysis, WiringStatus::Ready, 1_800_000_000)
        .await
        .expect("register flow loop");
    handle
}

/// One complete flow pass, exactly in the flow owner's order: begin (bump
/// generation), write rows, record the terminal. `generation` is stamped
/// into `forward_count` so a reader can tell WHICH pass produced the row
/// it is holding.
async fn one_pass(handle: &ObserverHandle, now: i64) {
    let generation = handle
        .begin_loop_pass(LoopId::FlowAnalysis, THIS_BOOT, now)
        .await
        .expect("begin pass");
    handle
        .upsert_channel_flow_state(row(SCID, THIS_BOOT, generation as i64))
        .await
        .expect("persist flow state");
    handle
        .finish_loop_pass(LoopId::FlowAnalysis, generation, THIS_BOOT, now)
        .await
        .expect("finish pass");
}

#[tokio::test]
async fn a_completed_pass_serves_the_row_it_wrote() {
    let dir = tempfile::tempdir().unwrap();
    let handle = observer(&dir).await;
    one_pass(&handle, 1_800_000_000).await;

    let evidence = current_boot_flow_evidence(Some(&handle), SCID, THIS_BOOT)
        .await
        .expect("a completed current-boot pass is evidence");
    match evidence {
        FlowEvidence::Current(got) => {
            assert_eq!(got.scid, SCID);
            assert_eq!(got.boot_id, THIS_BOOT);
        }
        FlowEvidence::NoSuchChannel => panic!("the pass wrote this channel"),
    }
}

#[tokio::test]
async fn before_the_first_pass_the_previous_boots_rows_are_refused() {
    // The store is warm from a previous process and the flow loop has not
    // run yet in this one. Every row on disk is another process's view of
    // the node.
    let dir = tempfile::tempdir().unwrap();
    let handle = observer(&dir).await;
    handle
        .upsert_channel_flow_state(row(SCID, PRIOR_BOOT, 99))
        .await
        .unwrap();

    let refusal = current_boot_flow_evidence(Some(&handle), SCID, THIS_BOOT)
        .await
        .expect_err("no pass has completed in this boot");
    assert_eq!(
        refusal,
        FlowEvidenceRefusal::NoPassThisBoot(BootStatus::NeverRunThisBoot),
        "a warm store must not be mistaken for a completed pass"
    );
}

#[tokio::test]
async fn a_channel_a_completed_pass_did_not_write_is_pythons_null() {
    let dir = tempfile::tempdir().unwrap();
    let handle = observer(&dir).await;
    one_pass(&handle, 1_800_000_000).await;

    let evidence = current_boot_flow_evidence(Some(&handle), "999x9x9", THIS_BOOT)
        .await
        .expect("a completed pass that does not track a channel is a real answer");
    assert_eq!(evidence, FlowEvidence::NoSuchChannel);
}

#[tokio::test]
async fn a_process_with_no_observer_store_refuses_permanently() {
    let refusal = current_boot_flow_evidence(None, SCID, THIS_BOOT)
        .await
        .expect_err("no store means no evidence can ever exist");
    assert_eq!(refusal, FlowEvidenceRefusal::StoreNotConfigured);
}

/// C71-14. The read must be ONE observation, and this is what it costs
/// when it is not.
///
/// A flow pass mutates BOTH halves the gate reasons about, in this order:
/// bump the generation, write the rows, record the terminal. So a reader
/// that fetches loop health and the channel row with two separate awaits
/// has a window the actor can schedule a whole pass into.
///
/// The test drives that window deterministically rather than hoping a race
/// shows up under load: it performs the first read, lets a pass begin and
/// write its row, then performs the second read. Both halves are real --
/// each was true when it was read -- but the PAIR describes a node state
/// that never existed, and the gate cannot tell, because the gate only
/// ever sees the pair.
///
/// The atomic command, given the very same store state, refuses.
#[tokio::test]
async fn a_torn_read_serves_an_in_flight_pass_under_a_completed_passs_authority() {
    let dir = tempfile::tempdir().unwrap();
    let handle = observer(&dir).await;

    // Generation 1 completes: terminal_generation = 1, row stamped 1.
    one_pass(&handle, 1_800_000_000).await;

    // --- the torn read, first half: health says a pass has completed.
    let torn_health = handle
        .list_loop_health()
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.loop_id == LoopId::FlowAnalysis);
    assert_eq!(
        torn_health.as_ref().unwrap().terminal_generation,
        1,
        "precondition: generation 1 has completed"
    );

    // --- the window: generation 2 begins and writes its row. It has NOT
    //     reached a terminal, so nothing about it is evidence yet.
    let generation = handle
        .begin_loop_pass(LoopId::FlowAnalysis, THIS_BOOT, 1_800_000_001)
        .await
        .unwrap();
    handle
        .upsert_channel_flow_state(row(SCID, THIS_BOOT, generation as i64))
        .await
        .unwrap();

    // --- the torn read, second half: the row now belongs to generation 2.
    let torn_row = handle
        .channel_flow_states()
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.scid == SCID);

    let torn =
        revops::flow_evidence::classify_flow_evidence(torn_health.as_ref(), torn_row, THIS_BOOT);
    match torn {
        Ok(FlowEvidence::Current(got)) => {
            assert_eq!(
                got.forward_count, 2,
                "the torn pair serves generation 2's half-written row"
            );
            assert_eq!(
                torn_health.unwrap().terminal_generation,
                1,
                "...under generation 1's completed-pass authority"
            );
        }
        other => {
            panic!("expected the torn read to hand back evidence it should not have; got {other:?}")
        }
    }

    // --- ONE observation of the identical store state refuses, because
    //     the generation and the row are read together and disagree.
    let snapshot = handle.flow_evidence_snapshot(SCID).await.unwrap();
    let atomic = revops::flow_evidence::classify_flow_evidence(
        snapshot.flow_loop.as_ref(),
        snapshot.row,
        THIS_BOOT,
    );
    assert_eq!(
        atomic,
        Err(FlowEvidenceRefusal::NoPassThisBoot(BootStatus::Incomplete)),
        "the atomic read must see the in-flight generation the torn read missed"
    );
}

/// The same guarantee under real contention: a reader racing a writer must
/// never pair a completed pass with a row another pass wrote. Each pass
/// stamps its generation into `forward_count`, and a pass reaches `Passed`
/// only after its row is written, so `Passed` evidence must always carry
/// the terminal generation.
///
/// This one cannot fail deterministically -- it is a soak over the atomic
/// path, not the proof. The proof is the test above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_passes_never_produce_inconsistent_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let handle = observer(&dir).await;
    one_pass(&handle, 1_800_000_000).await;

    let writer_handle = handle.clone();
    let writer = tokio::spawn(async move {
        for i in 0..400i64 {
            one_pass(&writer_handle, 1_800_000_000 + i).await;
        }
    });

    let mut served = 0u32;
    for _ in 0..400 {
        let snapshot = handle
            .flow_evidence_snapshot(SCID)
            .await
            .expect("snapshot read");
        let flow_loop = snapshot.flow_loop.clone();
        let evidence = revops::flow_evidence::classify_flow_evidence(
            flow_loop.as_ref(),
            snapshot.row,
            THIS_BOOT,
        );
        if let Ok(FlowEvidence::Current(got)) = evidence {
            let loop_row = flow_loop.expect("evidence implies a loop row");
            assert_eq!(
                got.forward_count, loop_row.terminal_generation as i64,
                "served a row from pass {} under the authority of completed pass {}",
                got.forward_count, loop_row.terminal_generation
            );
            served += 1;
        }
        tokio::task::yield_now().await;
    }

    writer.await.expect("writer task");
    assert!(
        served > 0,
        "the reader never once observed a completed pass, so this soak covered nothing"
    );
}
