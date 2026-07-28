//! Task 61 4F — END-TO-END local-fake LN+ lifecycle through the FULL
//! production stack: kernel watcher → gated ports (Armed, test-only) →
//! concrete adapters (`LnPlusApiClient<UreqTransport, ClnSigner>` +
//! `ClnChainAdapter`) → real `SqliteLnPlusDb`, against a scriptable TCP
//! fake LN+ server and a scriptable Unix-socket fake CLN. Loopback only;
//! no test here ever leaves 127.0.0.1 or a tempdir socket.
//!
//! Leg A: applied → opening (connect+fundchannel, attempt/reservation/
//! receipt exactly-once) → opened → active (no_close protection) →
//! ended (positive rating) — with a double-pass proving no re-fund.
//! Leg B: fundchannel hangs → OutcomeUnknown quarantine (reservation
//! HELD) → store survives a REAL reopen → reconciliation refuses a
//! wrong-capacity channel, then resolves exactly-once from matching
//! chain evidence — fundchannel submitted exactly ONCE across the run.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use revops::lnplus_adapters::{ClnChainAdapter, ClnSigner};
use revops_lnplus::exec_mode::ExecutionMode;
use revops_lnplus::http::LnPlusApiClient;
use revops_lnplus::http_ureq::UreqTransport;
use revops_lnplus::loop_drivers::watcher_pass;
use revops_lnplus::open::OpenExecParams;
use revops_lnplus::ports::{AttemptState, LnPlusDb, PeerPolicy, PolicyPort, PortResult};
use revops_lnplus::sqlite_db::SqliteLnPlusDb;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

// ------------------------------- scriptable local fakes (loopback only)

struct ScriptedHttpServer {
    url_base: String,
    routes: Arc<Mutex<BTreeMap<String, Value>>>,
    hits: Arc<Mutex<BTreeMap<String, usize>>>,
    _shutdown: std::sync::mpsc::Sender<()>,
}

impl ScriptedHttpServer {
    fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        listener.set_nonblocking(true).unwrap();
        let url_base = format!("http://{}", listener.local_addr().unwrap());
        let routes: Arc<Mutex<BTreeMap<String, Value>>> = Arc::new(Mutex::new(BTreeMap::new()));
        let hits: Arc<Mutex<BTreeMap<String, usize>>> = Arc::new(Mutex::new(BTreeMap::new()));
        let routes_task = routes.clone();
        let hits_task = hits.clone();
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        std::thread::spawn(move || loop {
            if rx.try_recv() != Err(std::sync::mpsc::TryRecvError::Empty) {
                return;
            }
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream.set_nonblocking(false).unwrap();
                    let request = read_http_request(&mut stream);
                    let path = request
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or("")
                        .split('?')
                        .next()
                        .unwrap_or("")
                        .to_string();
                    *hits_task.lock().unwrap().entry(path.clone()).or_insert(0) += 1;
                    let (status, body) = match routes_task.lock().unwrap().get(path.as_str()) {
                        Some(v) => (200, serde_json::to_vec(v).unwrap()),
                        None => (500, b"{\"error\":\"unexpected path\"}".to_vec()),
                    };
                    let head = format!(
                        "HTTP/1.1 {status} X\r\ncontent-length: {}\r\n\
                         content-type: application/json\r\nconnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(head.as_bytes());
                    let _ = stream.write_all(&body);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => return,
            }
        });
        Self {
            url_base,
            routes,
            hits,
            _shutdown: tx,
        }
    }

    fn set_route(&self, path: &str, body: Value) {
        self.routes.lock().unwrap().insert(path.to_string(), body);
    }

    fn hits_for(&self, path: &str) -> usize {
        self.hits.lock().unwrap().get(path).copied().unwrap_or(0)
    }
}

fn read_http_request(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(1) => buf.push(byte[0]),
            _ => break,
        }
    }
    let head = String::from_utf8_lossy(&buf).to_string();
    let content_length: usize = head
        .lines()
        .find_map(|l| {
            let (name, value) = l.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().ok())?
        })
        .unwrap_or(0);
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        let _ = stream.read_exact(&mut body);
    }
    format!("{head}{}", String::from_utf8_lossy(&body))
}

/// Method-scriptable fake CLN: per-method JSON results (mutable between
/// passes), per-method call counts, and a hang switch for `fundchannel`.
struct ScriptedCln {
    _dir: tempfile::TempDir,
    path: PathBuf,
    results: Arc<Mutex<BTreeMap<String, Value>>>,
    calls: Arc<Mutex<BTreeMap<String, usize>>>,
    hang_fundchannel: Arc<AtomicBool>,
    _rt: tokio::runtime::Runtime,
}

impl ScriptedCln {
    fn spawn() -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lightning-rpc");
        let results: Arc<Mutex<BTreeMap<String, Value>>> = Arc::new(Mutex::new(BTreeMap::new()));
        let calls: Arc<Mutex<BTreeMap<String, usize>>> = Arc::new(Mutex::new(BTreeMap::new()));
        let hang_fundchannel = Arc::new(AtomicBool::new(false));
        let listener = {
            let _g = rt.enter();
            UnixListener::bind(&path).unwrap()
        };
        let results_task = results.clone();
        let calls_task = calls.clone();
        let hang_task = hang_fundchannel.clone();
        rt.spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let results_task = results_task.clone();
                let calls_task = calls_task.clone();
                let hang_task = hang_task.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 8192];
                    loop {
                        let n = stream.read(&mut chunk).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        if let Ok(v) = serde_json::from_slice::<Value>(&buf) {
                            let method = v
                                .get("method")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string();
                            *calls_task
                                .lock()
                                .unwrap()
                                .entry(method.clone())
                                .or_insert(0) += 1;
                            if method == "fundchannel" && hang_task.load(Ordering::SeqCst) {
                                std::future::pending::<()>().await;
                            }
                            let result = results_task
                                .lock()
                                .unwrap()
                                .get(&method)
                                .cloned()
                                .unwrap_or_else(|| match method.as_str() {
                                    "signmessage" => json!({"zbase": "zsig"}),
                                    "listpeerchannels" => json!({"channels": []}),
                                    "getinfo" => json!({"id": "02abc"}),
                                    _ => json!({}),
                                });
                            let body = json!({"jsonrpc": "2.0", "id": 1, "result": result});
                            let mut out = serde_json::to_vec(&body).unwrap();
                            out.extend_from_slice(b"\n\n");
                            let _ = stream.write_all(&out).await;
                            return;
                        }
                    }
                });
            }
        });
        Self {
            _dir: dir,
            path,
            results,
            calls,
            hang_fundchannel,
            _rt: rt,
        }
    }

    fn set_result(&self, method: &str, result: Value) {
        self.results
            .lock()
            .unwrap()
            .insert(method.to_string(), result);
    }

    fn calls_for(&self, method: &str) -> usize {
        self.calls.lock().unwrap().get(method).copied().unwrap_or(0)
    }
}

// ------------------------------- minimal in-test policy + logger fakes

struct OkPolicy;
impl PolicyPort for OkPolicy {
    fn get_policy(&self, _peer: &str) -> PortResult<Option<Box<dyn PeerPolicy>>> {
        Ok(None)
    }
    fn add_tag(&self, _peer: &str, _tag: &str) -> PortResult<()> {
        Ok(())
    }
    fn remove_tag(&self, _peer: &str, _tag: &str) -> PortResult<()> {
        Ok(())
    }
    fn is_peer_banned(&self, _pubkey: &str) -> PortResult<bool> {
        Ok(false)
    }
}

struct TestLogger;
impl revops_lnplus::ports::Logger for TestLogger {
    fn log(&self, level: revops_lnplus::ports::LogLevel, message: &str) {
        eprintln!("e2e[{level:?}]: {message}");
    }
}

// --------------------------------------------------- shared e2e plumbing

struct Stack {
    server: ScriptedHttpServer,
    cln: ScriptedCln,
    store: SqliteLnPlusDb,
    store_path: PathBuf,
    _store_dir: tempfile::TempDir,
    api: LnPlusApiClient<UreqTransport, ClnSigner>,
    chain: ClnChainAdapter,
}

fn pubkey(seed: u8) -> String {
    format!("02{:064x}", seed as u128)
}

fn stack(rpc_timeout: Duration) -> Stack {
    let server = ScriptedHttpServer::spawn();
    let cln = ScriptedCln::spawn();
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("lnplus-e2e.db");
    let store = SqliteLnPlusDb::open(&store_path, Box::new(TestLogger)).unwrap();
    let signer = ClnSigner::new(cln.path.clone(), rpc_timeout).unwrap();
    let transport = UreqTransport::with_timeout(Duration::from_secs(5));
    let api =
        LnPlusApiClient::with_base_url(transport, signer, format!("{}/api/2", server.url_base));
    let chain = ClnChainAdapter::new(cln.path.clone(), rpc_timeout).unwrap();
    server.set_route("/api/2/get_message", json!({"message": "lnplus login 1"}));
    Stack {
        server,
        cln,
        store,
        store_path,
        _store_dir: dir,
        api,
        chain,
    }
}

fn open_exec() -> OpenExecParams {
    OpenExecParams {
        estimated_cost_sats: 2000,
        effective_budget_sats: None,
        budget_since_timestamp: None,
    }
}

fn run_watcher(stack: &Stack, now: i64) -> revops_lnplus::watcher::WatcherSummary {
    watcher_pass(
        // Armed is TEST-ONLY here: this is the point of the e2e — the
        // full armed lifecycle against fakes. Observer composition never
        // holds this mode (action_surface tripwires).
        ExecutionMode::Armed,
        &stack.store,
        &stack.api,
        &stack.chain,
        &OkPolicy,
        None,
        &TestLogger,
        &open_exec(),
        7,
        now,
    )
    .expect("watcher pass")
}

fn reservation_status(path: &PathBuf, rid_prefix: &str) -> Vec<(String, String)> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT reservation_id, status FROM spend_reservations WHERE reservation_id LIKE ?1",
        )
        .unwrap();
    let rows = stmt
        .query_map([format!("{rid_prefix}%")], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    rows.map(|r| r.unwrap()).collect()
}

fn receipt_count(path: &PathBuf) -> i64 {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM spend_events WHERE event_id LIKE 'resv:lnplus-open-%'",
        [],
        |r| r.get(0),
    )
    .unwrap()
}

// ================================ Leg A: full lifecycle applied → ended

#[test]
fn e2e_full_lifecycle_applied_to_ended_with_exactly_once_settlement() {
    let s = stack(Duration::from_secs(5));
    let peer_out = pubkey(1);
    let peer_in = pubkey(2);
    let now0 = 1_700_000_000i64;

    // Pre-state: an applied row past the reconcile grace window, backfill
    // choke already done (flag set), evaluator not involved (its planner
    // rail is Task 62 — the lifecycle starts from the committed apply).
    s.store
        .set_config_override(revops_lnplus::reconcile::BACKFILL_FLAG, "1")
        .unwrap();
    s.store
        .insert_swap_new(
            &revops_lnplus::db_types::SwapRow::new("s1", "applied", 1_000_000, 6, now0 - 700)
                .with_outbound_peer(peer_out.clone())
                .with_incoming_peer(peer_in.clone()),
        )
        .unwrap();

    // -- Pass 1: LN+ lists s1 as opening; the channel funds on chain.
    s.server.set_route(
        "/api/2/get_my_swaps",
        json!({"pending": [], "opening": [
            {"id": "s1", "outgoing_peer_pubkey": peer_out,
             "outgoing_peer_clearnet_address": "203.0.113.9:9735",
             "deadline": (now0 + 48*3600)}
        ], "completed": []}),
    );
    s.server
        .set_route("/api/2/complete_application", json!({"ok": true}));
    s.cln
        .set_result("fundchannel", json!({"txid": "e2e-txid-1"}));

    let summary = run_watcher(&s, now0);
    assert_eq!(summary.opened, vec!["s1".to_string()]);
    let row = s.store.get_swap("s1").unwrap();
    assert_eq!(row.status, "opened");
    assert_eq!(row.channel_funding_txid.as_deref(), Some("e2e-txid-1"));
    assert_eq!(s.cln.calls_for("connect"), 1);
    assert_eq!(s.cln.calls_for("fundchannel"), 1);
    // Attempt/reservation/receipt landed exactly once, atomically.
    let attempts = {
        let conn = rusqlite::Connection::open(&s.store_path).unwrap();
        conn.query_row(
            "SELECT COUNT(*) FROM lnplus_attempts WHERE swap_id='s1' AND state='committed'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
    };
    assert_eq!(attempts, 1);
    let reservations = reservation_status(&s.store_path, "lnplus-open-s1");
    assert_eq!(reservations.len(), 1);
    assert_eq!(reservations[0].1, "spent");
    assert_eq!(receipt_count(&s.store_path), 1);

    // -- Pass 1b: SAME opening listing again — must NOT fund twice.
    let summary = run_watcher(&s, now0 + 60);
    assert_eq!(
        s.cln.calls_for("fundchannel"),
        1,
        "an already-funded swap must never re-fund"
    );
    assert!(summary.opened.is_empty() || summary.opened == vec!["s1".to_string()]);

    // -- Pass 2: LN+ reports the ring completed; activation protects both
    // sides and the contract goes active.
    let ends_at = now0 + 600;
    s.server.set_route(
        "/api/2/get_my_swaps",
        json!({"pending": [], "opening": [], "completed": [
            {"id": "s1", "incoming_peer_pubkey": peer_in, "ends": ends_at}
        ]}),
    );
    let summary = run_watcher(&s, now0 + 120);
    assert_eq!(summary.activated, vec!["s1".to_string()]);
    let row = s.store.get_swap("s1").unwrap();
    assert_eq!(row.status, "active");
    assert_eq!(row.ends_at, Some(ends_at));

    // -- Pass 3: contract end reached; the incoming channel is still open
    // → positive rating → ended. Exactly one rating request.
    s.cln.set_result(
        "listpeerchannels",
        json!({"channels": [
            {"peer_id": peer_in, "state": "CHANNELD_NORMAL",
             "total_msat": 1_000_000_000i64, "to_us_msat": 0,
             "funding_txid": "their-txid"}
        ]}),
    );
    s.server
        .set_route("/api/2/create_rating", json!({"ok": true}));
    let summary = run_watcher(&s, ends_at + 10);
    assert_eq!(summary.finalized, vec!["s1".to_string()]);
    let row = s.store.get_swap("s1").unwrap();
    assert_eq!(row.status, "ended");
    assert_eq!(row.outcome.as_deref(), Some("positive"));
    assert_eq!(s.server.hits_for("/api/2/create_rating"), 1);
    // Settlement stayed exactly-once across the whole lifecycle.
    assert_eq!(receipt_count(&s.store_path), 1);
}

// ==== Leg B: outcome-unknown quarantine, restart survival, exactly-once
// ==== reconciliation from chain evidence

#[test]
fn e2e_unknown_outcome_quarantine_survives_restart_and_reconciles_exactly_once() {
    let s = stack(Duration::from_millis(400));
    let peer_out = pubkey(3);
    let now0 = 1_700_100_000i64;
    s.store
        .set_config_override(revops_lnplus::reconcile::BACKFILL_FLAG, "1")
        .unwrap();
    s.store
        .insert_swap_new(
            &revops_lnplus::db_types::SwapRow::new("s2", "applied", 1_000_000, 6, now0 - 700)
                .with_outbound_peer(peer_out.clone()),
        )
        .unwrap();
    s.server.set_route(
        "/api/2/get_my_swaps",
        json!({"pending": [], "opening": [
            {"id": "s2", "outgoing_peer_pubkey": peer_out,
             "deadline": (now0 + 48*3600)}
        ], "completed": []}),
    );

    // -- Pass 1: fundchannel HANGS past the adapter budget → typed
    // OutcomeUnknown → quarantine. The pass itself completes.
    s.cln.hang_fundchannel.store(true, Ordering::SeqCst);
    let summary = run_watcher(&s, now0);
    assert!(summary.opened.is_empty());
    assert_eq!(s.cln.calls_for("fundchannel"), 1);
    let unknowns = s.store.unknown_attempts().unwrap();
    assert_eq!(unknowns.len(), 1);
    assert_eq!(unknowns[0].state, AttemptState::OutcomeUnknown);
    let held = reservation_status(&s.store_path, "lnplus-open-s2");
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].1, "active", "the reservation is HELD while unknown");

    // -- RESTART: drop and reopen the real store file.
    let store_path = s.store_path.clone();
    drop(s.store);
    let store = SqliteLnPlusDb::open(&store_path, Box::new(TestLogger)).unwrap();
    assert_eq!(
        store.unknown_attempts().unwrap().len(),
        1,
        "quarantine survives restart"
    );

    // Rebuild the stack half around the reopened store.
    let s = Stack {
        store,
        store_path,
        ..s
    };
    s.cln.hang_fundchannel.store(false, Ordering::SeqCst);

    // -- Pass 2: chain answers with a MALFORMED channel row (missing
    // total_msat). The strict decode (F4C-1) makes that an ERROR, never
    // "absence": reconciliation must stay quarantined (no release on
    // garbage evidence) and the open path must fail closed (F4C-2 — no
    // second fundchannel against unreadable channel state).
    s.cln.set_result(
        "listpeerchannels",
        json!({"channels": [
            {"peer_id": peer_out, "state": "CHANNELD_NORMAL",
             "to_us_msat": 42_000_000i64, "funding_txid": "unrelated-txid"}
        ]}),
    );
    let _ = run_watcher(&s, now0 + 60);
    assert_eq!(
        s.store.unknown_attempts().unwrap().len(),
        1,
        "malformed chain evidence must never resolve the quarantine"
    );
    assert_eq!(
        s.cln.calls_for("fundchannel"),
        1,
        "unreadable channel state blocks any open — no resubmit"
    );
    assert_eq!(
        reservation_status(&s.store_path, "lnplus-open-s2")[0].1,
        "active",
        "the hold survives garbage evidence"
    );

    // -- Pass 3: chain now shows the MATCHING channel with its txid —
    // reconciliation resolves committed exactly once; the open path then
    // completes the application.
    s.cln.set_result(
        "listpeerchannels",
        json!({"channels": [
            {"peer_id": peer_out, "state": "CHANNELD_AWAITING_LOCKIN",
             "total_msat": 1_000_000_000i64, "to_us_msat": 1_000_000_000i64,
             "funding_txid": "found-on-chain"}
        ]}),
    );
    s.server
        .set_route("/api/2/complete_application", json!({"ok": true}));
    let summary = run_watcher(&s, now0 + 120);
    assert!(s.store.unknown_attempts().unwrap().is_empty());
    let row = s.store.get_swap("s2").unwrap();
    assert_eq!(row.channel_funding_txid.as_deref(), Some("found-on-chain"));
    assert_eq!(row.status, "opened");
    assert_eq!(summary.opened, vec!["s2".to_string()]);
    let settled = reservation_status(&s.store_path, "lnplus-open-s2");
    assert_eq!(settled[0].1, "spent");
    assert_eq!(receipt_count(&s.store_path), 1, "settled exactly once");

    // -- Pass 4: replay — nothing settles or funds twice.
    let _ = run_watcher(&s, now0 + 180);
    assert_eq!(s.cln.calls_for("fundchannel"), 1);
    assert_eq!(receipt_count(&s.store_path), 1);
}
