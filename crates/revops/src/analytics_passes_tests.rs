//! Task 71 / F71-R16 slice 2 (RED): the three CONCRETE analytics observer
//! passes.
//!
//! R16's finding is that the branch supplies library evidence kernels with
//! no production path: `flow_owner::run_flow_pass`,
//! `startup_snapshot::plan_startup_snapshot` and
//! `financial_snapshot::plan_financial_snapshot` are complete, tested, and
//! called by NOTHING outside tests, so FlowAnalysis / StartupSnapshot /
//! FinancialSnapshot stay `NotWired` on a running node. A loop that is
//! never installed never fails, which is why this was invisible.
//!
//! F71-R17 pins HOW to close it: `ObserverPass` stays `pub(crate)` (so
//! these tests live in-crate rather than in `tests/`), each subsystem adds
//! its OWN concrete pass type, and the vetted builders accept
//! `Arc<ConcreteType>` exactly as `with_fee`/`with_lnplus` already do. No
//! `dyn` builder, no public trait.
//!
//! Every pass here is OBSERVATION-ONLY and holds no action capability:
//! they read RPC, run frozen kernels, and route the result to the
//! Rust-owned observer store. The Task 69 boundary is untouched.

use std::path::PathBuf;
use std::sync::Arc;

use revops_db::loop_health::{current_boot_status, BootStatus, LoopId, WiringStatus};
use serde_json::{json, Value};

use crate::analytics_passes::{FinancialSnapshotPass, FlowAnalysisPass, StartupSnapshotPass};
use crate::loop_health::{LoopHealthStore, RequestKey};
use crate::runtime::{ObserverPassSet, ObserverRuntime};

const NOW: i64 = 1_800_000_000;
const BOOT: &str = "boot-under-test";
const PEER: &str = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

// ---------------------------------------------------------------------
// local fake `lightning-rpc` socket (repo-local only; no live CLN)
// ---------------------------------------------------------------------

/// Serve the mock socket, answering by method name with `cln_rpc`'s
/// framing. Same shape as `tests/fee_evidence.rs::serve_methods`.
fn serve(socket_path: PathBuf, replies: Vec<(&'static str, Value)>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::UnixListener::bind(&socket_path).expect("bind mock rpc socket");
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut buf = Vec::new();
            let mut chunk = [0u8; 8192];
            let req: Value = loop {
                let n = stream.read(&mut chunk).await.unwrap_or(0);
                if n == 0 {
                    return;
                }
                buf.extend_from_slice(&chunk[..n]);
                if let Ok(v) = serde_json::from_slice::<Value>(&buf) {
                    break v;
                }
            };
            let method = req.get("method").and_then(Value::as_str).unwrap_or("");
            let id = req.get("id").cloned().unwrap_or(Value::Null);
            let body = match replies.iter().find(|(m, _)| *m == method) {
                Some((_, result)) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
                None => json!({"jsonrpc": "2.0", "id": id,
                    "error": {"code": -32601, "message": format!("no fake for {method}")}}),
            };
            let mut out = serde_json::to_vec(&body).expect("encode reply");
            out.extend_from_slice(b"\n\n");
            let _ = stream.write_all(&out).await;
            let _ = stream.flush().await;
        }
    });
}

fn peer_channels() -> Value {
    json!({"channels": [{
        "state": "CHANNELD_NORMAL",
        "short_channel_id": "100x1x0",
        "peer_id": PEER,
        "total_msat": 1_000_000_000_i64,
        "to_us_msat": 400_000_000_i64,
    }]})
}

async fn observer_db(dir: &tempfile::TempDir) -> revops_db::owner::ObserverHandle {
    revops_db::owner::spawn_read_write(&dir.path().join("observer.sqlite3"))
        .await
        .expect("spawn observer db")
}

// ---------------------------------------------------------------------
// FlowAnalysisPass
// ---------------------------------------------------------------------

/// The load-bearing assertion R16 asks for: a real pass, driven through
/// the real loop owner, leaves THIS BOOT's derived state in the intended
/// Rust-owned store. A pass that ran but wrote nowhere is indistinguishable
/// from one that never ran.
#[tokio::test]
async fn flow_analysis_pass_routes_derived_state_to_the_observer_store_under_this_boot() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    serve(socket.clone(), vec![("listpeerchannels", peer_channels())]);
    let observer = observer_db(&dir).await;

    let pass = Arc::new(FlowAnalysisPass::new(
        socket,
        observer.clone(),
        BOOT.to_string(),
        crate::analytics_passes::FlowPassConfig::default(),
    ));

    crate::loop_health::ObserverPass::run(pass.as_ref(), RequestKey::from("test"))
        .await
        .expect("a readable snapshot must produce a completed pass");

    let states = observer.channel_flow_states().await.unwrap();
    let row = states
        .iter()
        .find(|s| s.scid == "100x1x0")
        .expect("the pass must route derived flow state to the observer store");
    assert_eq!(row.peer_id, PEER);
    assert_eq!(
        row.boot_id, BOOT,
        "the row must carry THIS process's boot id, not a prior boot's"
    );
    assert!(
        !observer.kalman_states().await.unwrap().is_empty(),
        "the filter state must persist, or every boot silently re-initializes it"
    );
}

/// An unreadable required source is a typed refusal that FAILS the pass —
/// never an empty success that would record a confident zero-flow fleet.
#[tokio::test]
async fn flow_analysis_pass_fails_the_pass_when_peer_channels_is_unreadable() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    // No `listpeerchannels` fake: the RPC errors.
    serve(socket.clone(), vec![("getinfo", json!({"id": PEER}))]);
    let observer = observer_db(&dir).await;

    let pass = FlowAnalysisPass::new(
        socket,
        observer.clone(),
        BOOT.to_string(),
        crate::analytics_passes::FlowPassConfig::default(),
    );

    let err = crate::loop_health::ObserverPass::run(&pass, RequestKey::from("test"))
        .await
        .expect_err("an unreadable channel snapshot must fail the pass");
    assert!(
        format!("{err:#}").contains("flow_peer_channels_unavailable"),
        "the failure must carry the typed refusal code, got: {err:#}"
    );
    assert!(
        observer.channel_flow_states().await.unwrap().is_empty(),
        "a refused pass must write NO state at all"
    );
}

/// The Kalman state persisted by one pass must be the state the next pass
/// resumes from. `KalmanFlowState` is not serde-derivable (frozen crate),
/// so the encode/decode goes field-by-field through the canonical
/// `to_dict`/`from_dict` — a silent field drop here would reset the filter
/// on every pass while still looking like it persisted.
#[tokio::test]
async fn flow_analysis_pass_round_trips_every_kalman_field_across_passes() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    serve(socket.clone(), vec![("listpeerchannels", peer_channels())]);
    let observer = observer_db(&dir).await;

    let pass = FlowAnalysisPass::new(
        socket,
        observer.clone(),
        BOOT.to_string(),
        crate::analytics_passes::FlowPassConfig::default(),
    );
    crate::loop_health::ObserverPass::run(&pass, RequestKey::from("first"))
        .await
        .unwrap();

    let after_first = observer.kalman_states().await.unwrap();
    let (_, encoded, _) = after_first
        .iter()
        .find(|(scid, _, _)| scid == "100x1x0")
        .expect("first pass persisted a filter state");
    for field in [
        "flow_ratio",
        "flow_velocity",
        "variance_ratio",
        "variance_velocity",
        "covariance",
        "last_update",
        "innovation_variance",
        "last_innovation",
        "observation_count",
    ] {
        assert!(
            encoded.get(field).is_some(),
            "kalman encoding dropped `{field}`; the filter would silently reset each pass"
        );
    }
}

// ---------------------------------------------------------------------
// StartupSnapshotPass
// ---------------------------------------------------------------------

/// py `_snapshot_peers_once`: record a connection event for every
/// CONNECTED peer with no history in the last hour. Disconnected peers and
/// already-recorded peers are skipped, not recorded-as-zero.
#[tokio::test]
async fn startup_snapshot_pass_records_connected_peers_without_recent_history() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    let other = format!("02{}", "bb".repeat(32));
    serve(
        socket.clone(),
        vec![(
            "listpeers",
            json!({"peers": [
                {"id": PEER, "connected": true},
                {"id": other, "connected": false},
            ]}),
        )],
    );
    let observer = observer_db(&dir).await;

    let pass = StartupSnapshotPass::new(socket, observer.clone(), NOW);
    crate::loop_health::ObserverPass::run(&pass, RequestKey::from("once"))
        .await
        .expect("a readable peer list must produce a completed one-shot pass");

    let recent = observer
        .peers_with_recent_connection_history(NOW - 3_600)
        .await
        .unwrap();
    assert!(
        recent.contains(PEER),
        "the connected peer must have a recorded snapshot event"
    );
    assert!(
        !recent.contains(&other),
        "a DISCONNECTED peer must not be recorded as connected"
    );
}

/// An unreadable peer list is a typed refusal, never an empty list that
/// would look like "no peers needed recording".
#[tokio::test]
async fn startup_snapshot_pass_fails_the_pass_when_peers_are_unreadable() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    serve(socket.clone(), vec![("getinfo", json!({"id": PEER}))]);
    let observer = observer_db(&dir).await;

    let pass = StartupSnapshotPass::new(socket, observer.clone(), NOW);
    let err = crate::loop_health::ObserverPass::run(&pass, RequestKey::from("once"))
        .await
        .expect_err("an unreadable peer list must fail the pass");
    assert!(
        format!("{err:#}").contains("startup_snapshot_peers_unavailable"),
        "the failure must carry the typed refusal code, got: {err:#}"
    );
}

// ---------------------------------------------------------------------
// FinancialSnapshotPass
// ---------------------------------------------------------------------

/// The snapshot row must reach `rust_financial_snapshots` with py's
/// arithmetic: `capacity_sats` COMPUTED as local+remote, revenue msat
/// floor-divided to sats.
#[tokio::test]
async fn financial_snapshot_pass_writes_a_row_with_python_arithmetic() {
    let dir = tempfile::tempdir().unwrap();
    let observer = observer_db(&dir).await;

    let pass = FinancialSnapshotPass::for_tests(
        observer.clone(),
        Ok(json!({
            "local_balance_sats": 700_000,
            "remote_balance_sats": 300_000,
            "onchain_sats": 50_000,
            "channel_count": 4,
        })),
        Ok(crate::financial_snapshot::LifetimeStats {
            total_revenue_msat: 1_999,
            total_rebalance_cost_sats: 120,
        }),
        NOW,
    );
    crate::loop_health::ObserverPass::run(&pass, RequestKey::from("daily"))
        .await
        .expect("complete evidence must produce a completed pass");

    let rows = observer.financial_snapshots(10).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].capacity_sats, 1_000_000);
    assert_eq!(
        rows[0].revenue_accumulated_sats, 1,
        "1_999 msat floors to 1"
    );
    assert_eq!(rows[0].channel_count, 4);
}

/// A missing required TLV field refuses; it must never become a
/// zero-balance snapshot that reads like a genuinely empty node.
#[tokio::test]
async fn financial_snapshot_pass_refuses_incomplete_tlv_rather_than_recording_zeros() {
    let dir = tempfile::tempdir().unwrap();
    let observer = observer_db(&dir).await;

    let pass = FinancialSnapshotPass::for_tests(
        observer.clone(),
        Ok(json!({"local_balance_sats": 700_000})),
        Ok(crate::financial_snapshot::LifetimeStats {
            total_revenue_msat: 0,
            total_rebalance_cost_sats: 0,
        }),
        NOW,
    );
    let err = crate::loop_health::ObserverPass::run(&pass, RequestKey::from("daily"))
        .await
        .expect_err("incomplete TLV must fail the pass");
    assert!(format!("{err:#}").contains("financial_snapshot_tlv_unavailable"));
    assert!(
        observer.financial_snapshots(10).await.unwrap().is_empty(),
        "a refused snapshot must write NO row"
    );
}

// ---------------------------------------------------------------------
// production composition — R17's definition of done
// ---------------------------------------------------------------------

/// R17: "a Ready registration with no request trigger is still not a
/// current-boot completed owner." This drives the REAL
/// `ObserverPassSet` -> `ObserverRuntime::start` path and then requires
/// the loop to reach a COMPLETED generation this boot.
#[tokio::test]
async fn composed_analytics_loops_are_ready_and_complete_a_current_boot_generation() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    serve(socket.clone(), vec![("listpeerchannels", peer_channels())]);
    let observer = observer_db(&dir).await;
    let store = Arc::new(LoopHealthStore::new(observer.clone(), BOOT.to_string()));

    let passes = ObserverPassSet::empty().with_flow_analysis(Arc::new(FlowAnalysisPass::new(
        socket,
        observer.clone(),
        BOOT.to_string(),
        crate::analytics_passes::FlowPassConfig::default(),
    )));
    let runtime = ObserverRuntime::start(
        crate::fee_mode::ObserverMode::for_tests(true),
        store,
        passes,
    )
    .await
    .expect("compose the analytics observer runtime");

    let handle = runtime
        .handle(LoopId::FlowAnalysis)
        .expect("FlowAnalysis must be WIRED once a concrete pass is composed");

    let rows = observer.list_loop_health().await.unwrap();
    let row = rows
        .iter()
        .find(|r| r.loop_id == LoopId::FlowAnalysis)
        .unwrap();
    assert_eq!(row.wiring_status, WiringStatus::Ready);
    assert_eq!(
        current_boot_status(row, BOOT),
        BootStatus::NeverRunThisBoot,
        "registration alone is NOT a completed owner"
    );

    handle.request(RequestKey::from("cadence")).await.unwrap();
    handle.wait_idle().await;

    let rows = observer.list_loop_health().await.unwrap();
    let row = rows
        .iter()
        .find(|r| r.loop_id == LoopId::FlowAnalysis)
        .unwrap();
    assert_eq!(
        current_boot_status(row, BOOT),
        BootStatus::Passed,
        "a triggered pass must complete a generation under THIS boot id"
    );
}
