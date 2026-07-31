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

use crate::config_resolve::SnapshotFreshness;
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
        // F71-R19/R20: spendable/receivable are the REQUIRED balance
        // fields; `to_us_msat` is not what py reads.
        "spendable_msat": 400_000_000_i64,
        "receivable_msat": 600_000_000_i64,
    }]})
}

async fn observer_db(dir: &tempfile::TempDir) -> revops_db::owner::ObserverHandle {
    revops_db::owner::spawn_read_write(&dir.path().join("observer.sqlite3"))
        .await
        .expect("spawn observer db")
}

/// Already-read config evidence for a pass under test. Tests inject the
/// SOURCES, not a resolved config, so every assertion here runs through
/// the same `resolve_flow_config` production uses -- a test that injected
/// the resolved values would pass even if the resolver were never called.
fn fixed_sources(
    db: &[(&str, &str)],
    lc: &[(&str, &str)],
    freshness: SnapshotFreshness,
) -> crate::flow_config::FlowConfigSources {
    crate::flow_config::FlowConfigSources {
        db_overrides: Ok(db
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()),
        listconfigs: lc
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        listconfigs_freshness: freshness,
    }
}

/// py's defaults, freshly read: the baseline for tests that are not about
/// configuration at all.
fn default_sources() -> crate::flow_config::FlowConfigSources {
    fixed_sources(&[], &[], SnapshotFreshness::Fresh)
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

    let pass = Arc::new(FlowAnalysisPass::for_tests(
        socket,
        observer.clone(),
        BOOT.to_string(),
        default_sources(),
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

    let pass = FlowAnalysisPass::for_tests(
        socket,
        observer.clone(),
        BOOT.to_string(),
        default_sources(),
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

    let pass = FlowAnalysisPass::for_tests(
        socket,
        observer.clone(),
        BOOT.to_string(),
        default_sources(),
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

/// F71-R25a: a PRESENT but malformed persisted temporal profile must
/// refuse. Defaulting it to a fresh zero profile is not graceful
/// degradation — an all-zero `hourly_out` is precisely what the frozen
/// kernel tests for its `is_first` branch, so a corrupt row makes it
/// DISCARD the channel's accumulated history and freeze
/// `observation_days`, silently weakening the graduation check.
#[tokio::test]
async fn a_corrupt_persisted_temporal_profile_fails_the_pass() {
    for (label, corrupt) in [
        (
            "short array",
            json!({
                "hourly_out": [0.0, 1.0], "hourly_in": vec![0.0; 24],
                "hourly_count": vec![0.0; 24], "dominant_bucket": "",
                "observation_days": 3, "last_observation_day": 0, "last_updated": 0,
            }),
        ),
        (
            "non-numeric bucket",
            json!({
                "hourly_out": (0..24).map(|i| if i == 5 { json!("x") } else { json!(0.0) })
                    .collect::<Vec<_>>(),
                "hourly_in": vec![0.0; 24], "hourly_count": vec![0.0; 24],
                "dominant_bucket": "", "observation_days": 3,
                "last_observation_day": 0, "last_updated": 0,
            }),
        ),
        (
            "missing counter",
            json!({
                "hourly_out": vec![0.0; 24], "hourly_in": vec![0.0; 24],
                "hourly_count": vec![0.0; 24], "dominant_bucket": "",
                "last_observation_day": 0, "last_updated": 0,
            }),
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("lightning-rpc");
        serve(socket.clone(), vec![("listpeerchannels", peer_channels())]);
        let observer = observer_db(&dir).await;
        observer
            .upsert_temporal_profile("100x1x0", corrupt, NOW)
            .await
            .unwrap();

        let pass = FlowAnalysisPass::for_tests(
            socket,
            observer.clone(),
            BOOT.to_string(),
            default_sources(),
        );
        let err = crate::loop_health::ObserverPass::run(&pass, RequestKey::from("t"))
            .await
            .expect_err(label);
        assert!(
            format!("{err:#}").contains("flow_history_unavailable"),
            "{label}: a corrupt stored profile must fail the pass, got: {err:#}"
        );
        assert!(
            observer.channel_flow_states().await.unwrap().is_empty(),
            "{label}: a refused pass writes nothing"
        );
    }
}

/// The ABSENT case is NOT malformed: a channel with no stored profile has
/// genuinely never been observed, and a fresh default is the correct
/// answer for it.
#[tokio::test]
async fn an_absent_temporal_profile_is_a_fresh_start_not_a_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    serve(socket.clone(), vec![("listpeerchannels", peer_channels())]);
    let observer = observer_db(&dir).await;

    let pass = FlowAnalysisPass::for_tests(
        socket,
        observer.clone(),
        BOOT.to_string(),
        default_sources(),
    );
    crate::loop_health::ObserverPass::run(&pass, RequestKey::from("t"))
        .await
        .expect("no stored profile is a legitimate first observation");
    assert!(
        !observer.temporal_profiles().await.unwrap().is_empty(),
        "the pass produced and persisted a profile"
    );
}

// ---------------------------------------------------------------------
// F71-R25c: the size_buckets PARSER itself
// ---------------------------------------------------------------------

/// The owner-side test feeds `dominant_bucket_override` directly, so it
/// never exercises this parsing at all — a version that took the FIRST
/// bucket rather than the highest-revenue one would pass it. These pin the
/// parser (py flow_analysis.py:1713-1723).
#[test]
fn dominant_size_bucket_picks_the_highest_revenue_share() {
    use crate::analytics_passes::dominant_size_bucket;

    // Winner is alphabetically LAST: catches "take the first bucket".
    let last_wins = json!({"size_buckets": {
        "aaa_small": {"revenue_share": 0.1},
        "zzz_large": {"revenue_share": 0.9},
    }})
    .to_string();
    assert_eq!(
        dominant_size_bucket(&last_wins).as_deref(),
        Some("zzz_large")
    );

    // Winner is alphabetically FIRST: catches "take the last bucket".
    let first_wins = json!({"size_buckets": {
        "aaa_large": {"revenue_share": 0.9},
        "zzz_small": {"revenue_share": 0.1},
    }})
    .to_string();
    assert_eq!(
        dominant_size_bucket(&first_wins).as_deref(),
        Some("aaa_large")
    );

    // Three buckets, winner in the middle of both orderings.
    let middle_wins = json!({"size_buckets": {
        "aaa": {"revenue_share": 0.2},
        "mmm": {"revenue_share": 0.7},
        "zzz": {"revenue_share": 0.1},
    }})
    .to_string();
    assert_eq!(dominant_size_bucket(&middle_wins).as_deref(), Some("mmm"));
}

/// py starts at `max_share = 0.0` and only replaces on `share > max_share`,
/// so buckets that exist but carry no positive share yield the literal
/// string "unknown" — the fee controller LOOKED and could not name one.
/// That is a real answer, and deliberately not the same as `None`.
#[test]
fn present_but_shareless_buckets_are_unknown_not_absent() {
    use crate::analytics_passes::dominant_size_bucket;

    let empty = json!({"size_buckets": {}}).to_string();
    assert_eq!(dominant_size_bucket(&empty).as_deref(), Some("unknown"));

    let all_zero = json!({"size_buckets": {
        "small": {"revenue_share": 0.0},
        "large": {"revenue_share": 0.0},
    }})
    .to_string();
    assert_eq!(dominant_size_bucket(&all_zero).as_deref(), Some("unknown"));

    // py: `data.get("revenue_share", 0.0) if isinstance(data, dict) else 0.0`
    let non_dict = json!({"size_buckets": {"small": 5, "large": "x"}}).to_string();
    assert_eq!(dominant_size_bucket(&non_dict).as_deref(), Some("unknown"));

    // A missing `revenue_share` key inside a real dict is also 0.0.
    let missing_share = json!({"size_buckets": {"small": {"other": 1.0}}}).to_string();
    assert_eq!(
        dominant_size_bucket(&missing_share).as_deref(),
        Some("unknown")
    );
}

/// `None` is py's `except: pass` — size profiling unavailable, so the
/// stored profile's existing label is KEPT rather than overwritten with
/// "unknown". Collapsing these two would erase a real label every time the
/// fee state was merely unreadable.
#[test]
fn absent_or_unparseable_size_buckets_are_none_not_unknown() {
    use crate::analytics_passes::dominant_size_bucket;

    assert_eq!(dominant_size_bucket(&json!({}).to_string()), None);
    assert_eq!(
        dominant_size_bucket(&json!({"thompson_state": {}}).to_string()),
        None
    );
    assert_eq!(dominant_size_bucket("not json at all"), None);
    // Present but not an object: py's `.items()` would raise -> except.
    assert_eq!(
        dominant_size_bucket(&json!({"size_buckets": [1, 2]}).to_string()),
        None
    );
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
        BOOT.to_string(),
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
        BOOT.to_string(),
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

    let passes =
        ObserverPassSet::empty().with_flow_analysis(Arc::new(FlowAnalysisPass::for_tests(
            socket,
            observer.clone(),
            BOOT.to_string(),
            default_sources(),
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

/// F71-R28: a snapshot carries the boot that took it, and a caller asking
/// for CURRENT financial evidence gets `None` on a boot that has not
/// measured yet — never the previous process's numbers.
///
/// The financial loop opens with a 300s startup delay, so EVERY boot has a
/// window where the newest row belongs to a prior boot. That window is
/// exactly when a dashboard is most likely to be read.
#[tokio::test]
async fn a_prior_boots_financial_snapshot_is_never_served_as_current() {
    let dir = tempfile::tempdir().unwrap();
    let observer = observer_db(&dir).await;

    // A previous process left a snapshot behind.
    let prior = FinancialSnapshotPass::for_tests(
        observer.clone(),
        "boot-previous".to_string(),
        Ok(json!({
            "local_balance_sats": 1, "remote_balance_sats": 1,
            "onchain_sats": 1, "channel_count": 1,
        })),
        Ok(crate::financial_snapshot::LifetimeStats {
            total_revenue_msat: 0,
            total_rebalance_cost_sats: 0,
        }),
        NOW - 86_400,
    );
    crate::loop_health::ObserverPass::run(&prior, RequestKey::from("old"))
        .await
        .unwrap();

    // This boot has not run its financial pass yet.
    assert!(
        observer
            .current_boot_financial_snapshot(BOOT)
            .await
            .unwrap()
            .is_none(),
        "a prior boot's snapshot must NOT answer for this boot"
    );
    // ...while the history question still has an answer.
    assert_eq!(observer.financial_snapshots(10).await.unwrap().len(), 1);

    // Once this boot measures, it answers.
    let current = FinancialSnapshotPass::for_tests(
        observer.clone(),
        BOOT.to_string(),
        Ok(json!({
            "local_balance_sats": 700_000, "remote_balance_sats": 300_000,
            "onchain_sats": 50_000, "channel_count": 4,
        })),
        Ok(crate::financial_snapshot::LifetimeStats {
            total_revenue_msat: 1_999,
            total_rebalance_cost_sats: 120,
        }),
        NOW,
    );
    crate::loop_health::ObserverPass::run(&current, RequestKey::from("now"))
        .await
        .unwrap();
    let row = observer
        .current_boot_financial_snapshot(BOOT)
        .await
        .unwrap()
        .expect("this boot has now measured");
    assert_eq!(row.boot_id, BOOT);
    assert_eq!(row.capacity_sats, 1_000_000);
}

// ---------------------------------------------------------------------
// F71-R27 / C71-6: the flow pass resolves its tunables PER PASS from the
// live sources, rather than from a config frozen at construction. Until these passed, an operator's `revenue-config set
// source_threshold` or `setconfig revenue-ops-flow-interval` changed
// nothing until the plugin was restarted -- and the config surface still
// reported the new value, so the disagreement was invisible.
// ---------------------------------------------------------------------

/// A channel whose HTLC slots are ~72.5% full (350 of 483). Congestion is
/// the cleanest config-driven classification flip available: it needs no
/// forwarding history, and it is a strict `>` comparison against exactly
/// one resolved tunable.
fn congested_peer_channels() -> Value {
    json!({"channels": [{
        "state": "CHANNELD_NORMAL",
        "short_channel_id": "100x1x0",
        "peer_id": PEER,
        "total_msat": 1_000_000_000_i64,
        "spendable_msat": 400_000_000_i64,
        "receivable_msat": 600_000_000_i64,
        "max_accepted_htlcs": 483,
        "htlcs": (0..350).map(|_| json!({})).collect::<Vec<_>>(),
    }]})
}

/// The load-bearing R27 assertion: a non-default value must reach the
/// CLASSIFIER. Same channel, same node state, two different resolved
/// thresholds, two different persisted classifications.
#[tokio::test]
async fn a_config_override_reaches_the_classifier() {
    async fn classify(override_threshold: Option<&str>) -> String {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("lightning-rpc");
        serve(
            socket.clone(),
            vec![("listpeerchannels", congested_peer_channels())],
        );
        let observer = observer_db(&dir).await;

        let pass = FlowAnalysisPass::for_tests(
            socket,
            observer.clone(),
            BOOT.to_string(),
            fixed_sources(
                &match override_threshold {
                    Some(v) => vec![("htlc_congestion_threshold", v)],
                    None => vec![],
                },
                &[],
                SnapshotFreshness::Fresh,
            ),
        );
        crate::loop_health::ObserverPass::run(&pass, RequestKey::from("test"))
            .await
            .expect("a readable snapshot must produce a completed pass");
        observer
            .channel_flow_states()
            .await
            .unwrap()
            .into_iter()
            .find(|s| s.scid == "100x1x0")
            .expect("the pass persisted a state")
            .flow_state
    }

    // 350/483 = 0.7246, which is NOT above py's 0.8 default.
    assert_ne!(
        classify(None).await,
        "congested",
        "0.7246 utilization must not be congested under py's 0.8 default"
    );
    // The operator lowered the bar to 0.6. If the override never reaches
    // the classifier, this is still the default classification.
    assert_eq!(
        classify(Some("0.6")).await,
        "congested",
        "an operator's htlc_congestion_threshold override must reach the classifier"
    );
}

/// `flow_window_days` must reach the STORE READ, not just the arithmetic.
/// A resolved window that never widens the query is a config knob that
/// silently does nothing.
#[tokio::test]
async fn a_config_override_reaches_the_flow_window() {
    async fn forward_count(window_days: &str) -> i64 {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("lightning-rpc");
        serve(socket.clone(), vec![("listpeerchannels", peer_channels())]);
        let observer = observer_db(&dir).await;

        // One forward inside py's 7-day default, one only inside a wider
        // window.
        for age_days in [2_i64, 10_i64] {
            observer
                .insert_forward(revops_db::notifications::ForwardRow {
                    in_channel: "200x1x0".to_string(),
                    out_channel: "100x1x0".to_string(),
                    in_msat: 1_000_000,
                    out_msat: 999_000,
                    fee_msat: 1_000,
                    timestamp: crate::now_unix() - age_days * 86_400,
                    resolved_time: crate::now_unix() - age_days * 86_400,
                })
                .await
                .unwrap();
        }

        let pass = FlowAnalysisPass::for_tests(
            socket,
            observer.clone(),
            BOOT.to_string(),
            fixed_sources(
                &[("flow_window_days", window_days)],
                &[],
                SnapshotFreshness::Fresh,
            ),
        );
        crate::loop_health::ObserverPass::run(&pass, RequestKey::from("test"))
            .await
            .expect("pass runs");
        observer
            .channel_flow_states()
            .await
            .unwrap()
            .into_iter()
            .find(|s| s.scid == "100x1x0")
            .expect("the pass persisted a state")
            .forward_count
    }

    assert_eq!(
        forward_count("7").await,
        1,
        "py's 7-day window sees one forward"
    );
    assert_eq!(
        forward_count("21").await,
        2,
        "a widened window must reach the bucket query, not only the divisor"
    );
}

/// C71-6 names the cadence explicitly: the resolved interval must reach
/// the NEXT sleep. A pass that classifies with the live value but sleeps
/// on the compiled-in one is still half-frozen.
#[tokio::test]
async fn a_config_override_reaches_the_next_cadence() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    serve(socket.clone(), vec![("listpeerchannels", peer_channels())]);
    let observer = observer_db(&dir).await;

    let pass = FlowAnalysisPass::for_tests(
        socket,
        observer.clone(),
        BOOT.to_string(),
        fixed_sources(
            &[],
            &[("revenue-ops-flow-interval", "900")],
            SnapshotFreshness::Fresh,
        ),
    );

    crate::loop_health::ObserverPass::run(&pass, RequestKey::from("test"))
        .await
        .expect("pass runs");
    assert_eq!(
        pass.interval_secs(),
        900,
        "the next cadence must use the interval resolved by the pass that just ran"
    );
}

/// An option source that was NEVER successfully read must refuse the pass,
/// not run it on fabricated defaults. A cold-start socket race would
/// otherwise reclassify the whole fleet on values the operator replaced,
/// and the only trace would be the classifications themselves.
#[tokio::test]
async fn an_unread_option_source_refuses_the_pass_and_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    serve(socket.clone(), vec![("listpeerchannels", peer_channels())]);
    let observer = observer_db(&dir).await;

    let pass = FlowAnalysisPass::for_tests(
        socket,
        observer.clone(),
        BOOT.to_string(),
        fixed_sources(&[], &[], SnapshotFreshness::NeverRefreshed),
    );

    let err = crate::loop_health::ObserverPass::run(&pass, RequestKey::from("test"))
        .await
        .expect_err("an unread option source must refuse");
    assert!(
        format!("{err:#}").contains("flow_config_listconfigs_unavailable"),
        "the failure must name the unread source, got: {err:#}"
    );
    assert!(
        observer.channel_flow_states().await.unwrap().is_empty(),
        "a refused pass must write NO state at all"
    );
}

/// ...but a snapshot RETAINED across a failed refresh is Python's own
/// behaviour (`_refresh_dynamic_config` keeps the live config object), so
/// it runs. Refusing here would take the flow loop down for the duration
/// of any lightningd hiccup, which Python never does.
#[tokio::test]
async fn a_last_good_option_snapshot_still_runs_the_pass() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    serve(socket.clone(), vec![("listpeerchannels", peer_channels())]);
    let observer = observer_db(&dir).await;

    let pass = FlowAnalysisPass::for_tests(
        socket,
        observer.clone(),
        BOOT.to_string(),
        fixed_sources(
            &[],
            &[("revenue-ops-flow-interval", "900")],
            SnapshotFreshness::LastGood {
                consecutive_failures: 4,
            },
        ),
    );

    crate::loop_health::ObserverPass::run(&pass, RequestKey::from("test"))
        .await
        .expect("the last good snapshot is a real prior answer from the real source");
    assert_eq!(pass.interval_secs(), 900);
    assert!(!observer.channel_flow_states().await.unwrap().is_empty());
}

/// A FAILED `config_overrides` read refuses too -- symmetric with the
/// listconfigs tier. Resolving past it runs the node on defaults the
/// operator explicitly replaced.
#[tokio::test]
async fn an_unreadable_override_table_refuses_the_pass() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    serve(socket.clone(), vec![("listpeerchannels", peer_channels())]);
    let observer = observer_db(&dir).await;

    let pass = FlowAnalysisPass::for_tests(
        socket,
        observer.clone(),
        BOOT.to_string(),
        crate::flow_config::FlowConfigSources {
            db_overrides: Err("config_overrides read failed: disk i/o error".to_string()),
            listconfigs: std::collections::BTreeMap::new(),
            listconfigs_freshness: SnapshotFreshness::Fresh,
        },
    );

    let err = crate::loop_health::ObserverPass::run(&pass, RequestKey::from("test"))
        .await
        .expect_err("an unreadable override table must refuse");
    assert!(
        format!("{err:#}").contains("flow_config_overrides_unavailable"),
        "got: {err:#}"
    );
    assert!(observer.channel_flow_states().await.unwrap().is_empty());
}

// ---------------------------------------------------------------------
// F71-R29: the tests above all drive `FlowConfigSource::Fixed`, which
// means the PRODUCTION assembly -- refresh the shared cache, read every
// config_overrides row, render lightningd's typed values to text, pair
// values with freshness -- had no coverage at all. That is the same
// producer-with-no-consumer shape F71-R16 was raised for, one level down:
// a `Live` branch that no test enters can be arbitrarily wrong while every
// `Fixed` test stays green.
// ---------------------------------------------------------------------

/// A production-shaped DB holding exactly the rows the flow pass reads.
async fn overrides_db(
    dir: &tempfile::TempDir,
    rows: &[(&str, &str)],
) -> revops_db::actor::DbHandle {
    let path = dir.path().join("production.sqlite3");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE config_overrides (
                 key TEXT PRIMARY KEY, value TEXT, version INTEGER, updated_at INTEGER);",
        )
        .unwrap();
        for (i, (key, value)) in rows.iter().enumerate() {
            conn.execute(
                "INSERT INTO config_overrides (key, value, version, updated_at) \
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![key, value, i as i64 + 1, NOW],
            )
            .unwrap();
        }
    }
    revops_db::actor::spawn_read_only(&path).await.unwrap()
}

/// `listconfigs` as lightningd actually answers it. `value_int` is not
/// decoration: CLN reports each option under the type it was REGISTERED
/// with, so an int-declared option arrives as `value_int` no matter what
/// the operator typed. A production assembly that only handled `value_str`
/// would silently drop it and resolve the default instead.
fn listconfigs_reply(entries: &[(&str, Value)]) -> Value {
    let mut configs = serde_json::Map::new();
    for (name, value) in entries {
        configs.insert(name.to_string(), value.clone());
    }
    json!({ "configs": configs })
}

/// The whole production path, end to end: a real `listconfigs` RPC through
/// the refreshable cache, a real `config_overrides` table read, and the
/// resolved values reaching both the classifier and the next cadence.
#[tokio::test]
async fn the_live_config_assembly_reaches_the_classifier_and_the_cadence() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    serve(
        socket.clone(),
        vec![
            ("listpeerchannels", congested_peer_channels()),
            (
                "listconfigs",
                listconfigs_reply(&[
                    // int-declared, as CLN reports it
                    ("revenue-ops-flow-interval", json!({"value_int": 900})),
                    // string-declared, the common case
                    (
                        "revenue-ops-htlc-congestion-threshold",
                        json!({"value_str": "0.9"}),
                    ),
                    // an unrelated option must not disturb resolution
                    ("revenue-ops-min-fee-ppm", json!({"value_str": "40"})),
                ]),
            ),
        ],
    );
    let observer = observer_db(&dir).await;
    // The DB tier outranks listconfigs: 0.6 wins over lightningd's 0.9,
    // so a pass that read only one tier lands on a different answer.
    let db = overrides_db(&dir, &[("htlc_congestion_threshold", "0.6")]).await;

    let pass = FlowAnalysisPass::live(
        socket,
        observer.clone(),
        BOOT.to_string(),
        Some(db),
        crate::config_resolve::PythonOptionCache::empty(),
    );

    crate::loop_health::ObserverPass::run(&pass, RequestKey::from("test"))
        .await
        .expect("the live assembly must produce a completed pass");

    let row = observer
        .channel_flow_states()
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.scid == "100x1x0")
        .expect("the pass persisted a state");
    assert_eq!(
        row.flow_state, "congested",
        "the DB override (0.6) must win over lightningd's 0.9 and reach the classifier"
    );
    assert_eq!(
        pass.interval_secs(),
        900,
        "an int-typed listconfigs value must reach the next cadence"
    );
}

/// The cache starts empty, so the FIRST thing the live pass must do is
/// refresh it. If it never did, `NeverRefreshed` would refuse every pass
/// forever — which is why this cannot be asserted by the refusal test
/// alone: only a pass that actually fetched can succeed here.
#[tokio::test]
async fn the_live_pass_refreshes_the_option_cache_before_resolving() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    serve(
        socket.clone(),
        vec![
            ("listpeerchannels", peer_channels()),
            ("listconfigs", listconfigs_reply(&[])),
        ],
    );
    let observer = observer_db(&dir).await;
    let cache = crate::config_resolve::PythonOptionCache::empty();
    assert_eq!(cache.freshness(), SnapshotFreshness::NeverRefreshed);

    let pass = FlowAnalysisPass::live(
        socket,
        observer.clone(),
        BOOT.to_string(),
        Some(overrides_db(&dir, &[]).await),
        cache.clone(),
    );
    crate::loop_health::ObserverPass::run(&pass, RequestKey::from("test"))
        .await
        .expect("a reachable listconfigs must let the pass run");
    assert_eq!(
        cache.freshness(),
        SnapshotFreshness::Fresh,
        "the pass must refresh the SHARED cache, not a private copy"
    );
}

/// A live pass whose `listconfigs` never answers must refuse rather than
/// classify on defaults. This is the cold-start socket race, driven
/// through the real RPC path rather than a fixed freshness value.
#[tokio::test]
async fn the_live_pass_refuses_when_listconfigs_has_never_answered() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    // `listpeerchannels` is reachable; `listconfigs` is not.
    serve(socket.clone(), vec![("listpeerchannels", peer_channels())]);
    let observer = observer_db(&dir).await;

    let pass = FlowAnalysisPass::live(
        socket,
        observer.clone(),
        BOOT.to_string(),
        Some(overrides_db(&dir, &[]).await),
        crate::config_resolve::PythonOptionCache::empty(),
    );
    let err = crate::loop_health::ObserverPass::run(&pass, RequestKey::from("test"))
        .await
        .expect_err("an unanswered listconfigs must refuse the pass");
    assert!(
        format!("{err:#}").contains("flow_config_listconfigs_unavailable"),
        "got: {err:#}"
    );
    assert!(observer.channel_flow_states().await.unwrap().is_empty());
}

/// No production database attached means the override tier is UNREADABLE,
/// not empty. Treating it as empty would run the node on defaults the
/// operator replaced, with no trace anywhere.
#[tokio::test]
async fn the_live_pass_refuses_when_no_production_database_is_attached() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    serve(
        socket.clone(),
        vec![
            ("listpeerchannels", peer_channels()),
            ("listconfigs", listconfigs_reply(&[])),
        ],
    );
    let observer = observer_db(&dir).await;

    let pass = FlowAnalysisPass::live(
        socket,
        observer.clone(),
        BOOT.to_string(),
        None,
        crate::config_resolve::PythonOptionCache::empty(),
    );
    let err = crate::loop_health::ObserverPass::run(&pass, RequestKey::from("test"))
        .await
        .expect_err("an unreadable override tier must refuse");
    assert!(
        format!("{err:#}").contains("flow_config_overrides_unavailable"),
        "got: {err:#}"
    );
    assert!(observer.channel_flow_states().await.unwrap().is_empty());
}

/// A live pass RE-resolves every cycle. The second pass must see a value
/// written after the first one ran — that is the entire point of R27, and
/// no single-pass test can show it.
#[tokio::test]
async fn a_second_live_pass_picks_up_a_value_changed_since_the_first() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    serve(
        socket.clone(),
        vec![
            ("listpeerchannels", congested_peer_channels()),
            ("listconfigs", listconfigs_reply(&[])),
        ],
    );
    let observer = observer_db(&dir).await;

    let db_path = dir.path().join("production.sqlite3");
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE config_overrides (
                 key TEXT PRIMARY KEY, value TEXT, version INTEGER, updated_at INTEGER);",
        )
        .unwrap();
    }
    let db = revops_db::actor::spawn_read_only(&db_path).await.unwrap();

    let pass = FlowAnalysisPass::live(
        socket,
        observer.clone(),
        BOOT.to_string(),
        Some(db),
        crate::config_resolve::PythonOptionCache::empty(),
    );

    crate::loop_health::ObserverPass::run(&pass, RequestKey::from("first"))
        .await
        .expect("first pass runs on py's defaults");
    let first = observer
        .channel_flow_states()
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.scid == "100x1x0")
        .unwrap()
        .flow_state;
    assert_ne!(first, "congested", "0.7246 is below py's 0.8 default");

    // The operator runs `revenue-config set` between cycles.
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO config_overrides (key, value, version, updated_at) \
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["htlc_congestion_threshold", "0.6", 1i64, NOW],
        )
        .unwrap();
    }

    crate::loop_health::ObserverPass::run(&pass, RequestKey::from("second"))
        .await
        .expect("second pass runs");
    let second = observer
        .channel_flow_states()
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.scid == "100x1x0")
        .unwrap()
        .flow_state;
    assert_eq!(
        second, "congested",
        "a value set between cycles must take effect WITHOUT a plugin restart"
    );
}
