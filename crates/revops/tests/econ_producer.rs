//! C71-35: the econ-snapshot assembly's BEHAVIOUR, driven end to end.
//!
//! Every case here runs the real producer against real temp stores and a
//! fake CLN socket. Source-text assertions prove a call is written; these
//! prove what the caller actually receives.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use revops::econ_producer::{econ_snapshot_response, EconSources};
use revops::rpc_econ_snapshot::PROFITABILITY_UNAVAILABLE;
use revops_db::actor::spawn_read_only;
use revops_db::fee_runway::{FeeCycleCommit, FeeStateRow};
use revops_db::owner::{spawn_read_write, ObserverHandle};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const NOW: i64 = 1_800_000_000;
const DAY: i64 = 86_400;

const SCHEMA: &str = "
CREATE TABLE forwards (
    id INTEGER PRIMARY KEY, in_channel TEXT, out_channel TEXT,
    in_msat INTEGER, out_msat INTEGER, fee_msat INTEGER, timestamp INTEGER
);
CREATE TABLE daily_forwarding_stats (channel_id TEXT, date INTEGER, forward_count INTEGER);
CREATE TABLE daily_forwarding_stats_inbound (channel_id TEXT, date INTEGER, forward_count INTEGER);
CREATE TABLE rebalance_history (
    id INTEGER PRIMARY KEY, from_channel TEXT, to_channel TEXT,
    rebalance_type TEXT, status TEXT, timestamp INTEGER
);
CREATE TABLE channel_costs (
    channel_id TEXT PRIMARY KEY, peer_id TEXT, open_cost_sats INTEGER,
    capacity_sats INTEGER, opened_at INTEGER
);
CREATE TABLE rebalance_costs (
    id INTEGER PRIMARY KEY, channel_id TEXT, peer_id TEXT,
    cost_sats INTEGER, cost_msat INTEGER, amount_sats INTEGER, timestamp INTEGER
);
CREATE TABLE budget_reservations (
    reservation_id TEXT PRIMARY KEY, reserved_sats INTEGER,
    reserved_at INTEGER, job_channel_id TEXT, status TEXT
);
CREATE TABLE spend_reservations (
    reservation_id TEXT PRIMARY KEY, category TEXT, subcategory TEXT,
    reserved_sats INTEGER, reserved_at INTEGER, reference_id TEXT,
    channel_id TEXT, status TEXT, metadata_json TEXT
);
";

fn peer_id() -> String {
    format!("02{}", "aa".repeat(32))
}

/// A fake `lightning-rpc` that COUNTS how many `listpeerchannels` calls it
/// served. The count is the runtime proof that the assembly and the
/// profitability pass share one snapshot.
fn fake_cln(socket_path: std::path::PathBuf) -> Arc<AtomicUsize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    tokio::spawn(async move {
        let listener = tokio::net::UnixListener::bind(&socket_path).expect("bind fake rpc");
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let counter = counter.clone();
            tokio::spawn(async move {
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
                let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
                let id = req.get("id").cloned().unwrap_or(Value::Null);
                if method == "listpeerchannels" {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
                let body = json!({
                    "jsonrpc": "2.0", "id": id,
                    "result": {"channels": [{
                        "state": "CHANNELD_NORMAL",
                        "short_channel_id": "700x1x0",
                        "peer_id": peer_id(),
                        "opener": "remote",
                        "total_msat": 1_000_000_000_i64,
                        "to_us_msat": 400_000_000_i64,
                    }]}
                });
                // cln_rpc expects the reply terminated -- same framing the
                // other fakes in this crate use.
                let mut out = serde_json::to_vec(&body).unwrap();
                out.extend_from_slice(b"\n\n");
                let _ = stream.write_all(&out).await;
            });
        }
    });
    calls
}

struct Fixture {
    production: revops_db::actor::DbHandle,
    observer: ObserverHandle,
    socket: std::path::PathBuf,
    calls: Arc<AtomicUsize>,
    _dir: tempfile::TempDir,
}

async fn fixture(prod_schema: &str, seed: &str) -> Fixture {
    fixture_with_fee_rows(prod_schema, seed, vec![]).await
}

async fn fixture_with_fee_rows(
    prod_schema: &str,
    seed: &str,
    fee_rows: Vec<FeeStateRow>,
) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prod.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(prod_schema).unwrap();
        if !seed.is_empty() {
            conn.execute_batch(seed).unwrap();
        }
    }
    let production = spawn_read_only(&path).await.unwrap();
    let observer = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    if !fee_rows.is_empty() {
        observer
            .commit_fee_cycle(FeeCycleCommit {
                cycle_id: "c71-36-fixture".to_string(),
                started_at: NOW,
                completed_at: NOW,
                source_commit: "0".repeat(40),
                binary_sha256: "0".repeat(64),
                state_rows: fee_rows,
                ..Default::default()
            })
            .await
            .expect("seeds the fee state");
    }
    let socket = dir.path().join("lightning-rpc");
    let calls = fake_cln(socket.clone());
    // Let the listener bind before the first call.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    Fixture {
        production,
        observer,
        socket,
        calls,
        _dir: dir,
    }
}

fn seed_one_channel() -> String {
    format!(
        "INSERT INTO forwards (in_channel,out_channel,fee_msat,out_msat,timestamp)
         VALUES ('900x1x0','700x1x0', 5000, 1000000, {routed});
         INSERT INTO channel_costs (channel_id,peer_id,open_cost_sats,capacity_sats,opened_at)
         VALUES ('700x1x0','{peer}', 1000, 5000000, {opened});
         INSERT INTO rebalance_costs (channel_id,peer_id,cost_sats,amount_sats,timestamp)
         VALUES ('700x1x0','{peer}', 120, 0, {recent});
         INSERT INTO budget_reservations
             (reservation_id,reserved_sats,reserved_at,job_channel_id,status)
         VALUES ('r', 80, {recent}, '700x1x0', 'active');",
        routed = NOW - DAY,
        opened = NOW - 400 * DAY,
        recent = NOW - 3600,
        peer = peer_id(),
    )
}

fn sources<'a>(f: &'a Fixture, observer: Option<&'a ObserverHandle>) -> EconSources<'a> {
    EconSources {
        production_db: Some(&f.production),
        observer,
        socket_path: &f.socket,
        receivable_ratio_target: Ok(0.25),
        daily_budget_sats: Ok(50_000),
        enabled: Ok(true),
        now: NOW,
    }
}

// ---------------------------------------------------------------------
// enabled, full evidence
// ---------------------------------------------------------------------

#[tokio::test]
async fn enabled_with_full_evidence_assembles_a_real_snapshot() {
    let f = fixture(SCHEMA, &seed_one_channel()).await;
    let v = econ_snapshot_response(sources(&f, Some(&f.observer))).await;

    assert_eq!(v["enabled"], json!(true), "{v:?}");
    let snapshot = v["snapshot"].as_object().expect("a real snapshot");
    assert_eq!(
        snapshot["channels"].as_array().unwrap().len(),
        1,
        "the fetched channel is present: {snapshot:?}"
    );
    // The budget block carries the READ figures, not zeros. The wire is
    // msat-native, so the sats inputs appear scaled.
    let budget = &snapshot["node"]["daily_budget"];
    assert_eq!(budget["cap_msat"], json!(50_000_000), "{snapshot:?}");
    assert_eq!(budget["spent_msat"], json!(120_000));
    assert_eq!(budget["reserved_msat"], json!(80_000));
    // Nothing failed, so no evidence-failure line.
    let approximations: Vec<&str> = v["approximations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap())
        .collect();
    assert!(
        !approximations
            .iter()
            .any(|a| a.starts_with(PROFITABILITY_UNAVAILABLE)),
        "{approximations:?}"
    );
}

/// The runtime proof of the shared snapshot: the fake socket counts the
/// `listpeerchannels` calls a single request produced.
#[tokio::test]
async fn one_request_fetches_the_channel_snapshot_exactly_once() {
    let f = fixture(SCHEMA, &seed_one_channel()).await;
    let _ = econ_snapshot_response(sources(&f, Some(&f.observer))).await;

    assert_eq!(
        f.calls.load(Ordering::SeqCst),
        1,
        "the assembly and the profitability pass must share ONE snapshot; a \
         second fetch could disagree and nothing downstream could tell"
    );
}

// ---------------------------------------------------------------------
// declared degradation vs refusal
// ---------------------------------------------------------------------

#[tokio::test]
async fn a_profitability_failure_is_declared_and_contributes_no_evidence() {
    // No observer store: the fee posterior cannot be consulted at all.
    let f = fixture(SCHEMA, &seed_one_channel()).await;
    let v = econ_snapshot_response(sources(&f, None)).await;

    assert_eq!(v["enabled"], json!(true));
    assert!(
        v["snapshot"].is_object(),
        "Python DEGRADES here rather than refusing, and the shape must stay \
         Python's: {v:?}"
    );
    let approximations: Vec<&str> = v["approximations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap())
        .collect();
    assert!(
        approximations
            .iter()
            .any(|a| a.starts_with(PROFITABILITY_UNAVAILABLE)),
        "an absent-evidence snapshot must SAY so -- Python swallows this \
         silently: {approximations:?}"
    );
}

/// The other profitability-failure arm: the observer EXISTS, but the
/// gather itself refuses.
///
/// The test above passes `observer: None`, which takes a different branch
/// entirely -- so without this one, deleting the `Err(refusal)` arm's
/// declaration passes every test while silently assembling a snapshot that
/// looks fully evidenced.
#[tokio::test]
async fn a_refusing_profitability_read_is_declared_even_with_a_healthy_observer() {
    // `daily_forwarding_stats` is required by the profitability snapshot
    // but NOT by the budget read, so the budget still succeeds and the
    // pass reaches the profitability step, which then refuses.
    let schema = SCHEMA.replace(
        "CREATE TABLE daily_forwarding_stats (channel_id TEXT, date INTEGER, forward_count INTEGER);",
        "",
    );
    let f = fixture(&schema, "").await;
    let v = econ_snapshot_response(sources(&f, Some(&f.observer))).await;

    assert_eq!(v["enabled"], json!(true), "{v:?}");
    assert!(
        v["snapshot"].is_object(),
        "Python degrades rather than refusing here: {v:?}"
    );
    let approximations: Vec<&str> = v["approximations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap())
        .collect();
    assert!(
        approximations
            .iter()
            .any(|a| a.starts_with(PROFITABILITY_UNAVAILABLE)),
        "a refusing gather must declare itself: {approximations:?}"
    );
}

/// C71-36: a fleet-level `Ok` can still carry PER-CHANNEL refusals.
///
/// `gather_profitability` refuses one channel (corrupt fee posterior) and
/// returns `Ok` for the rest. That channel has no profitability entry, so
/// the snapshot shows it with ZERO economics -- identical to a channel
/// that genuinely earned and spent nothing. The skip must be declared, or
/// the response reads as fully evidenced when it is not.
#[tokio::test]
async fn a_per_channel_evidence_refusal_is_declared_even_when_the_fleet_pass_succeeds() {
    let f = fixture_with_fee_rows(
        SCHEMA,
        &seed_one_channel(),
        vec![FeeStateRow {
            channel_id: "700x1x0".to_string(),
            v2_state_json: "{not json".to_string(),
            last_update: NOW,
        }],
    )
    .await;
    let v = econ_snapshot_response(sources(&f, Some(&f.observer))).await;

    assert_eq!(v["enabled"], json!(true), "{v:?}");
    assert!(
        v["snapshot"].is_object(),
        "Python degrades per channel rather than refusing the fleet: {v:?}"
    );
    let approximations: Vec<&str> = v["approximations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap())
        .collect();
    let declared = approximations
        .iter()
        .find(|a| a.starts_with(revops::rpc_econ_snapshot::CHANNEL_EVIDENCE_SKIPPED))
        .unwrap_or_else(|| panic!("the skipped channel must be declared: {approximations:?}"));
    assert!(
        declared.contains("700x1x0"),
        "the declaration must name the channel: {declared}"
    );
    assert!(
        declared.contains("profitability_fee_state_unavailable"),
        "and the source that refused: {declared}"
    );

    // The channel is still IN the snapshot, with no economics -- which is
    // exactly why the note above has to exist.
    let channels = v["snapshot"]["channels"].as_array().expect("channels");
    assert_eq!(channels.len(), 1);
    assert_eq!(
        channels[0]["exit_revenue_msat"],
        json!(0),
        "{:?}",
        channels[0]
    );
}

/// Two skips, and the declarations are ORDERED.
///
/// A response whose approximation list reshuffles between identical calls
/// is a diff that never settles: an operator comparing two snapshots
/// cannot tell a real change from HashMap iteration order. One skipped
/// channel cannot show this -- order is only observable with two.
///
/// `800x1x0` has costs but is absent from `listpeerchannels`, so it has no
/// opener; `700x1x0` has a corrupt fee posterior. Different sources,
/// deterministic order.
#[tokio::test]
async fn multiple_skipped_channels_are_declared_in_a_stable_order() {
    let seed = format!(
        "{base}
         INSERT INTO channel_costs (channel_id,peer_id,open_cost_sats,capacity_sats,opened_at)
         VALUES ('800x1x0','{peer}', 1000, 5000000, {opened});",
        base = seed_one_channel(),
        opened = NOW - 400 * DAY,
        peer = peer_id(),
    );
    let f = fixture_with_fee_rows(
        SCHEMA,
        &seed,
        vec![FeeStateRow {
            channel_id: "700x1x0".to_string(),
            v2_state_json: "{not json".to_string(),
            last_update: NOW,
        }],
    )
    .await;
    let v = econ_snapshot_response(sources(&f, Some(&f.observer))).await;

    let declared: Vec<&str> = v["approximations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap())
        .filter(|a| a.starts_with(revops::rpc_econ_snapshot::CHANNEL_EVIDENCE_SKIPPED))
        .collect();
    assert_eq!(declared.len(), 2, "both channels are skipped: {declared:?}");
    assert!(declared[0].contains("700x1x0"), "{declared:?}");
    assert!(
        declared[1].contains("800x1x0"),
        "sorted by scid: {declared:?}"
    );
    assert!(
        declared[0].contains("profitability_fee_state_unavailable"),
        "{declared:?}"
    );
    assert!(
        declared[1].contains("profitability_opener_unavailable"),
        "{declared:?}"
    );
}

/// The control: a fully-evidenced pass declares no skips. Without it,
/// pushing a skip line unconditionally would pass the test above while
/// telling every healthy caller a channel was dropped.
#[tokio::test]
async fn a_fully_evidenced_pass_declares_no_skipped_channels() {
    let f = fixture(SCHEMA, &seed_one_channel()).await;
    let v = econ_snapshot_response(sources(&f, Some(&f.observer))).await;
    let approximations: Vec<&str> = v["approximations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap())
        .collect();
    assert!(
        !approximations
            .iter()
            .any(|a| a.starts_with(revops::rpc_econ_snapshot::CHANNEL_EVIDENCE_SKIPPED)),
        "{approximations:?}"
    );
}

#[tokio::test]
async fn an_unreadable_budget_refuses_rather_than_reporting_zeros() {
    // `rebalance_costs` is missing, so the budget read fails. Python
    // degrades to `{}` (zeros); zeros understate committed budget, which
    // is the one direction that can authorise spend.
    let schema = SCHEMA.replace(
        "CREATE TABLE rebalance_costs (
    id INTEGER PRIMARY KEY, channel_id TEXT, peer_id TEXT,
    cost_sats INTEGER, cost_msat INTEGER, amount_sats INTEGER, timestamp INTEGER
);",
        "",
    );
    let f = fixture(&schema, "").await;
    let v = econ_snapshot_response(sources(&f, Some(&f.observer))).await;

    assert_eq!(v["error"], json!("econ_budget_unavailable"), "{v:?}");
    assert!(v.get("snapshot").is_none(), "{v:?}");
    assert!(
        v.get("enabled").is_none(),
        "a refusal is not an enabled/disabled claim: {v:?}"
    );
}

#[tokio::test]
async fn an_unreadable_config_surface_refuses() {
    let f = fixture(SCHEMA, &seed_one_channel()).await;
    for (field, broken) in [
        ("receivable_ratio_target", true),
        ("daily_budget_sats", false),
    ] {
        let mut s = sources(&f, Some(&f.observer));
        if broken {
            s.receivable_ratio_target = Err("config_overrides read failed".to_string());
        } else {
            s.daily_budget_sats = Err("config_overrides read failed".to_string());
        }
        let v = econ_snapshot_response(s).await;
        assert_eq!(
            v["error"],
            json!("econ_config_unavailable"),
            "{field} must refuse, not default: {v:?}"
        );
    }
}

#[tokio::test]
async fn an_unreadable_gate_is_not_a_disabled_shadow() {
    let f = fixture(SCHEMA, "").await;
    let mut s = sources(&f, Some(&f.observer));
    s.enabled = Err("config_overrides read failed".to_string());
    let v = econ_snapshot_response(s).await;

    assert_eq!(v["error"], json!("econ_shadow_config_unavailable"), "{v:?}");
    assert!(
        v.get("enabled").is_none(),
        "neither a true nor a false enabled claim may be made: {v:?}"
    );
}

#[tokio::test]
async fn a_genuinely_disabled_shadow_returns_pythons_two_key_shape() {
    let f = fixture(SCHEMA, "").await;
    let mut s = sources(&f, Some(&f.observer));
    s.enabled = Ok(false);
    let v = econ_snapshot_response(s).await;

    assert_eq!(v["enabled"], json!(false));
    assert_eq!(
        v["hint"],
        json!("revenue-config set econ_shadow_enabled true")
    );
    assert!(v.get("snapshot").is_none(), "exactly two keys: {v:?}");
    // And nothing was fetched: the gate short-circuits before any I/O.
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_unconfigured_production_store_refuses() {
    let f = fixture(SCHEMA, "").await;
    let mut s = sources(&f, Some(&f.observer));
    s.production_db = None;
    let v = econ_snapshot_response(s).await;
    assert_eq!(v["error"], json!("econ_store_not_configured"), "{v:?}");
}

#[tokio::test]
async fn a_channel_read_failure_reports_it_without_fabricating_a_snapshot() {
    let f = fixture(SCHEMA, "").await;
    let mut s = sources(&f, Some(&f.observer));
    let missing = f.socket.parent().unwrap().join("no-such-socket");
    s.socket_path = &missing;
    let v = econ_snapshot_response(s).await;

    assert_eq!(v["enabled"], json!(true));
    assert_eq!(v["snapshot"], Value::Null);
    assert!(v["approximations"][0]
        .as_str()
        .unwrap()
        .starts_with("channel read failed"));
}

// ---------------------------------------------------------------------
// the counters stay shadow-mode nulls at this seam too
// ---------------------------------------------------------------------

#[tokio::test]
async fn the_assembled_response_still_declares_its_null_intent_counters() {
    let f = fixture(SCHEMA, &seed_one_channel()).await;
    let v = econ_snapshot_response(sources(&f, Some(&f.observer))).await;

    assert_eq!(v["intents_recorded_total"], Value::Null);
    assert_eq!(v["intents_ledger_total"], Value::Null);
    let approximations: Vec<&str> = v["approximations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap())
        .collect();
    assert!(
        approximations
            .iter()
            .any(|a| a.contains("intents_recorded_total unavailable")),
        "{approximations:?}"
    );
}

/// Guard against an empty-map fixture masking the evidence path: with a
/// healthy observer the channel really is evaluated.
#[tokio::test]
async fn the_profitability_evidence_actually_reaches_the_snapshot() {
    let f = fixture(SCHEMA, &seed_one_channel()).await;
    let v = econ_snapshot_response(sources(&f, Some(&f.observer))).await;
    let channels = v["snapshot"]["channels"].as_array().expect("channels");
    let entry = channels.first().expect("one channel");
    assert_eq!(entry["channel_id"], json!("700x1x0"), "{entry:?}");
    // The per-channel economics really were assembled, not defaulted: the
    // seeded forward's fee and the seeded rebalance cost both appear.
    assert_eq!(entry["exit_revenue_msat"], json!(5_000), "{entry:?}");
    assert_eq!(entry["rebalance_cost_msat"], json!(120_000), "{entry:?}");
}
