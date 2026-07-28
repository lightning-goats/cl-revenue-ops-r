//! Task 61 4D — the LN+ observer pass against LOCAL fakes only (TCP fake
//! LN+ server + Unix-socket fake CLN + temp store). Pins:
//!  - disabled (default) pass performs ZERO network calls;
//!  - enabled pass is WATCHER-ONLY: signed `get_my_swaps` observation,
//!    NEVER the evaluator's `get_applicable_swaps` (planner evidence rail
//!    not wired — no gates on fabricated economics), NEVER a mutating
//!    endpoint;
//!  - the observer adapter types refuse actions with ZERO wire activity.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use revops::lnplus_adapters::{ObserverClnChain, ObserverLnPlusApi};
use revops::lnplus_runtime::{LnPlusObserverPass, LnPlusRuntimeConfig, ENABLED_KEY};
use revops_lnplus::ports::{ChainPort, Feerate, LnPlusApi, LnPlusDb};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

// ------------------------------ routed local HTTP fake (loopback only)

struct RoutedHttpServer {
    url_base: String,
    hits: Arc<Mutex<BTreeMap<String, usize>>>,
    _shutdown: std::sync::mpsc::Sender<()>,
}

impl RoutedHttpServer {
    /// Serves `routes` (path → JSON body) forever until dropped; records
    /// per-path hit counts; unknown paths get a LOUD 500.
    fn spawn(routes: BTreeMap<&'static str, Value>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        listener.set_nonblocking(true).unwrap();
        let url_base = format!("http://{}", listener.local_addr().unwrap());
        let hits: Arc<Mutex<BTreeMap<String, usize>>> = Arc::new(Mutex::new(BTreeMap::new()));
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
                    let (status, body) = match routes.get(path.as_str()) {
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
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => return,
            }
        });
        Self {
            url_base,
            hits,
            _shutdown: tx,
        }
    }

    fn hits_for(&self, path: &str) -> usize {
        self.hits.lock().unwrap().get(path).copied().unwrap_or(0)
    }

    fn total_hits(&self) -> usize {
        self.hits.lock().unwrap().values().sum()
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

// ------------------------------ fake CLN (Unix socket, loopback fixture)

struct FakeCln {
    _dir: tempfile::TempDir,
    path: PathBuf,
    connections: Arc<AtomicUsize>,
    _rt: tokio::runtime::Runtime,
}

impl FakeCln {
    /// Answers every request by method: signmessage → zbase; everything
    /// else → an empty-but-valid shape.
    fn spawn() -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lightning-rpc");
        let connections = Arc::new(AtomicUsize::new(0));
        let listener = {
            let _g = rt.enter();
            UnixListener::bind(&path).unwrap()
        };
        let connections_task = connections.clone();
        rt.spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                connections_task.fetch_add(1, Ordering::SeqCst);
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
                            let method = v.get("method").and_then(Value::as_str).unwrap_or("");
                            let result = match method {
                                "signmessage" => json!({"zbase": "zsig"}),
                                "listpeerchannels" => json!({"channels": []}),
                                "getinfo" => json!({"id": "02abc"}),
                                _ => json!({}),
                            };
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
            connections,
            _rt: rt,
        }
    }
}

fn lnplus_routes() -> BTreeMap<&'static str, Value> {
    let mut routes = BTreeMap::new();
    routes.insert(
        "/api/2/get_message",
        json!({"message": "lnplus login 4242"}),
    );
    routes.insert(
        "/api/2/get_my_swaps",
        json!({"pending": [], "opening": [], "completed": []}),
    );
    // Present so a mutated evaluator COULD hit it — the test asserts it
    // never does.
    routes.insert("/api/2/get_applicable_swaps", json!({"swaps": []}));
    routes
}

fn pass_for(
    server: &RoutedHttpServer,
    cln: &FakeCln,
    store_dir: &tempfile::TempDir,
) -> Arc<LnPlusObserverPass> {
    LnPlusObserverPass::observer(LnPlusRuntimeConfig {
        store_path: store_dir.path().join("observer.db"),
        socket_path: cln.path.clone(),
        base_url: format!("{}/api/2", server.url_base),
        http_timeout: Duration::from_secs(5),
        rpc_timeout: Duration::from_secs(5),
    })
    .expect("build observer pass")
}

#[test]
fn disabled_pass_performs_zero_network_calls() {
    let server = RoutedHttpServer::spawn(lnplus_routes());
    let cln = FakeCln::spawn();
    let dir = tempfile::tempdir().unwrap();
    let pass = pass_for(&server, &cln, &dir);

    pass.run_once_blocking()
        .expect("disabled pass is a clean skip");

    assert_eq!(server.total_hits(), 0, "no LN+ HTTP contact while disabled");
    assert_eq!(
        cln.connections.load(Ordering::SeqCst),
        0,
        "no CLN contact while disabled"
    );
}

#[test]
fn enabled_pass_is_watcher_only_observation() {
    let server = RoutedHttpServer::spawn(lnplus_routes());
    let cln = FakeCln::spawn();
    let dir = tempfile::tempdir().unwrap();
    let pass = pass_for(&server, &cln, &dir);
    pass.store()
        .set_config_override(ENABLED_KEY, "true")
        .unwrap();

    pass.run_once_blocking().expect("enabled watcher pass");

    assert_eq!(
        server.hits_for("/api/2/get_my_swaps"),
        1,
        "the watcher observes via one signed get_my_swaps"
    );
    assert!(
        server.hits_for("/api/2/get_message") >= 1,
        "signed = challenged"
    );
    assert_eq!(
        server.hits_for("/api/2/get_applicable_swaps"),
        0,
        "the evaluator must NOT run: planner evidence rail is not wired — no gates on \
         fabricated economics"
    );
    assert_eq!(
        server.hits_for("/api/2/create_application")
            + server.hits_for("/api/2/delete_application")
            + server.hits_for("/api/2/complete_application")
            + server.hits_for("/api/2/create_rating")
            + server.hits_for("/api/2/mark_read_notifications"),
        0,
        "no mutating endpoint is ever touched by observation"
    );
    // Watcher side effects landed in the Rust store: the one-time
    // backfill choke point ran over the (empty) listing and set its flag.
    assert!(
        pass.store()
            .get_config_override(revops_lnplus::reconcile::BACKFILL_FLAG)
            .is_some(),
        "watcher really ran: backfill flag persisted in the LN+ store"
    );
}

// ---------------------------- observer adapter refusals (zero wire bytes)

#[test]
fn observer_chain_refuses_actions_with_zero_wire_activity() {
    let cln = FakeCln::spawn();
    let chain = ObserverClnChain::new(cln.path.clone(), Duration::from_secs(5)).unwrap();

    let err = chain.connect("02aa@203.0.113.9:9735").unwrap_err();
    assert!(err
        .to_string()
        .contains("observer runtime holds no LN+ action capability"));
    let err = chain
        .fund_channel("02aa", 1_000_000, Feerate::Normal)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("observer runtime holds no LN+ action capability"));
    assert_eq!(
        cln.connections.load(Ordering::SeqCst),
        0,
        "a refusal must not construct any RPC — zero connections"
    );

    // Control: reads DO work through the same type.
    assert_eq!(chain.our_node_id().unwrap(), "02abc");
    assert!(cln.connections.load(Ordering::SeqCst) > 0);
}

#[test]
fn observer_api_refuses_mutations_with_zero_wire_activity() {
    let server = RoutedHttpServer::spawn(lnplus_routes());
    let cln = FakeCln::spawn();
    let api = ObserverLnPlusApi::new(
        format!("{}/api/2", server.url_base),
        Duration::from_secs(5),
        cln.path.clone(),
        Duration::from_secs(5),
    )
    .unwrap();

    assert!(api.create_application("s1").is_err());
    assert!(api.delete_application("s1").is_err());
    assert!(api.complete_application("s1").is_err());
    assert!(api
        .create_rating("s1", revops_lnplus::types::Rating::Positive)
        .is_err());
    assert!(api.mark_read_notifications().is_err());
    assert_eq!(
        server.total_hits(),
        0,
        "a refusal must not issue ANY request — not even the auth challenge"
    );
    assert_eq!(
        cln.connections.load(Ordering::SeqCst),
        0,
        "no signing either"
    );

    // Control: a read works through the same type.
    let my = api.get_my_swaps().expect("signed read");
    assert!(my.pending.is_empty());
    assert!(server.hits_for("/api/2/get_my_swaps") == 1);
}
