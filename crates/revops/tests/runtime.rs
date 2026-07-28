use anyhow::{anyhow, Result};
use revops::loop_health::{Admission, LoopHandle, LoopHealthPersistence, ObserverPass, RequestKey};
use revops::runtime::{ObserverRuntime, REQUIRED_LOOPS};
use revops_db::loop_health::{LoopHealthRow, LoopId, WiringStatus};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, Semaphore};

#[derive(Default)]
struct MemoryStore {
    rows: Mutex<BTreeMap<LoopId, LoopHealthRow>>,
    fail_begin: AtomicBool,
    fail_terminal: AtomicBool,
    fail_backpressure: AtomicBool,
    writes: Mutex<Vec<&'static str>>,
}

impl MemoryStore {
    fn writes(&self) -> Vec<&'static str> {
        self.writes.lock().unwrap().clone()
    }
    fn row(&self, id: LoopId) -> LoopHealthRow {
        self.rows.lock().unwrap()[&id].clone()
    }
}

impl LoopHealthPersistence for MemoryStore {
    fn register<'a>(
        &'a self,
        id: LoopId,
        wiring: WiringStatus,
        now: i64,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.writes.lock().unwrap().push("register");
            self.rows
                .lock()
                .unwrap()
                .entry(id)
                .and_modify(|r| {
                    r.wiring_status = wiring;
                    r.updated_at = now;
                })
                .or_insert_with(|| LoopHealthRow::new(id, wiring, now));
            Ok(())
        })
    }
    fn reconcile<'a>(
        &'a self,
        _now: i64,
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send + 'a>> {
        Box::pin(async { Ok(0) })
    }
    fn begin<'a>(
        &'a self,
        id: LoopId,
        now: i64,
    ) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + 'a>> {
        Box::pin(async move {
            self.writes.lock().unwrap().push("begin");
            if self.fail_begin.load(Ordering::SeqCst) {
                return Err(anyhow!("begin unavailable"));
            }
            let mut rows = self.rows.lock().unwrap();
            let row = rows.get_mut(&id).unwrap();
            row.generation += 1;
            row.last_started_at = Some(now);
            row.updated_at = now;
            Ok(row.generation)
        })
    }
    fn finish<'a>(
        &'a self,
        id: LoopId,
        generation: u64,
        now: i64,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.writes.lock().unwrap().push("finish");
            if self.fail_terminal.load(Ordering::SeqCst) {
                return Err(anyhow!("finish unavailable"));
            }
            let mut rows = self.rows.lock().unwrap();
            let row = rows.get_mut(&id).unwrap();
            if row.generation != generation {
                return Err(anyhow!("stale generation"));
            }
            row.last_passed_at = Some(now);
            row.terminal_generation = generation;
            row.terminal_status = revops_db::loop_health::TerminalStatus::Passed;
            row.updated_at = now;
            Ok(())
        })
    }
    fn fail<'a>(
        &'a self,
        id: LoopId,
        generation: u64,
        now: i64,
        error: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.writes.lock().unwrap().push("fail");
            if self.fail_terminal.load(Ordering::SeqCst) {
                return Err(anyhow!("fail unavailable"));
            }
            let mut rows = self.rows.lock().unwrap();
            let row = rows.get_mut(&id).unwrap();
            if row.generation != generation {
                return Err(anyhow!("stale generation"));
            }
            row.last_error_at = Some(now);
            row.last_error = Some(error.to_string());
            row.terminal_generation = generation;
            row.terminal_status = revops_db::loop_health::TerminalStatus::Error;
            row.updated_at = now;
            Ok(())
        })
    }
    fn backpressure<'a>(
        &'a self,
        id: LoopId,
        coalesced: u64,
        dropped: u64,
        now: i64,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.writes.lock().unwrap().push(if coalesced > 0 {
                "coalesced"
            } else {
                "dropped"
            });
            if self.fail_backpressure.load(Ordering::SeqCst) {
                return Err(anyhow!("counter unavailable"));
            }
            let mut rows = self.rows.lock().unwrap();
            let row = rows.get_mut(&id).unwrap();
            row.coalesced_total += coalesced;
            row.dropped_total += dropped;
            row.updated_at = now;
            Ok(())
        })
    }
}

struct BlockingPass {
    running: AtomicUsize,
    maximum: AtomicUsize,
    calls: AtomicUsize,
    started: Notify,
    permits: Semaphore,
    fail: AtomicBool,
    panic: AtomicBool,
}

impl BlockingPass {
    fn new() -> Self {
        Self {
            running: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
            started: Notify::new(),
            permits: Semaphore::new(0),
            fail: AtomicBool::new(false),
            panic: AtomicBool::new(false),
        }
    }
}

impl ObserverPass for BlockingPass {
    fn run<'a>(
        &'a self,
        _key: RequestKey,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let n = self.running.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(n, Ordering::SeqCst);
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            let _permit = self.permits.acquire().await.unwrap();
            self.running.fetch_sub(1, Ordering::SeqCst);
            assert!(!self.panic.load(Ordering::SeqCst), "scripted pass panic");
            if self.fail.load(Ordering::SeqCst) {
                Err(anyhow!("scripted pass failure"))
            } else {
                Ok(())
            }
        })
    }
}

async fn one_loop(store: Arc<MemoryStore>, pass: Arc<BlockingPass>) -> LoopHandle {
    let mut passes: BTreeMap<LoopId, Arc<dyn ObserverPass>> = BTreeMap::new();
    passes.insert(LoopId::Fee, pass);
    ObserverRuntime::start(store, passes)
        .await
        .unwrap()
        .handle(LoopId::Fee)
        .unwrap()
}

#[tokio::test]
async fn registers_exact_five_identities_without_noop_owners() {
    let store = Arc::new(MemoryStore::default());
    let runtime = ObserverRuntime::start(store.clone(), BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(REQUIRED_LOOPS.len(), 5);
    assert_eq!(store.rows.lock().unwrap().len(), 5);
    for id in REQUIRED_LOOPS {
        assert_eq!(store.row(id).wiring_status, WiringStatus::NotWired);
        assert!(
            runtime.handle(id).is_none(),
            "unwired {id:?} must have no owner"
        );
    }
}

#[tokio::test]
async fn fake_passes_exercise_all_five_loop_identities() {
    let store = Arc::new(MemoryStore::default());
    let mut passes: BTreeMap<LoopId, Arc<dyn ObserverPass>> = BTreeMap::new();
    let mut concrete = Vec::new();
    for id in REQUIRED_LOOPS {
        let pass = Arc::new(BlockingPass::new());
        pass.permits.add_permits(1);
        passes.insert(id, pass.clone());
        concrete.push((id, pass));
    }
    let runtime = ObserverRuntime::start(store.clone(), passes).await.unwrap();
    for (id, pass) in concrete {
        let handle = runtime.handle(id).expect("wired fake handle");
        handle
            .request(RequestKey::from(format!("{id:?}")))
            .await
            .unwrap();
        handle.wait_idle().await;
        assert_eq!(pass.calls.load(Ordering::SeqCst), 1, "{id:?}");
        assert_eq!(store.row(id).terminal_generation, 1, "{id:?}");
    }
}

#[tokio::test]
async fn owner_is_single_flight_coalesces_duplicates_and_drops_ninth_pending_key() {
    let store = Arc::new(MemoryStore::default());
    let pass = Arc::new(BlockingPass::new());
    let handle = one_loop(store.clone(), pass.clone()).await;
    assert_eq!(
        handle.request(RequestKey::from("running")).await.unwrap(),
        Admission::Enqueued
    );
    pass.started.notified().await;
    assert_eq!(store.row(LoopId::Fee).generation, 1);
    assert_eq!(
        store.row(LoopId::Fee).last_passed_at,
        None,
        "dispatch/start is not completion"
    );
    assert_eq!(
        handle.request(RequestKey::from("running")).await.unwrap(),
        Admission::Coalesced
    );
    for i in 0..8 {
        assert_eq!(
            handle
                .request(RequestKey::from(format!("pending-{i}")))
                .await
                .unwrap(),
            Admission::Enqueued
        );
    }
    assert_eq!(
        handle.request(RequestKey::from("overflow")).await.unwrap(),
        Admission::Dropped
    );
    pass.permits.add_permits(9);
    handle.wait_idle().await;
    assert_eq!(pass.maximum.load(Ordering::SeqCst), 1);
    assert_eq!(store.row(LoopId::Fee).coalesced_total, 1);
    assert_eq!(store.row(LoopId::Fee).dropped_total, 1);
    let writes = store.writes();
    assert_eq!(writes.iter().filter(|w| **w == "begin").count(), 9);
    assert_eq!(writes.iter().filter(|w| **w == "finish").count(), 9);
    assert!(writes.contains(&"coalesced"));
    assert!(writes.contains(&"dropped"));
}

#[tokio::test]
async fn begin_failure_prevents_execution_and_terminal_failure_suspends() {
    let store = Arc::new(MemoryStore::default());
    let pass = Arc::new(BlockingPass::new());
    store.fail_begin.store(true, Ordering::SeqCst);
    let handle = one_loop(store.clone(), pass.clone()).await;
    handle.request(RequestKey::from("a")).await.unwrap();
    handle.wait_idle().await;
    assert_eq!(pass.calls.load(Ordering::SeqCst), 0);
    assert!(handle.is_suspended());
    assert_eq!(handle.abandoned_pending(), 1);
    assert_eq!(store.row(LoopId::Fee).dropped_total, 1);
    let store = Arc::new(MemoryStore::default());
    let pass = Arc::new(BlockingPass::new());
    store.fail_terminal.store(true, Ordering::SeqCst);
    let handle = one_loop(store.clone(), pass.clone()).await;
    handle.request(RequestKey::from("b")).await.unwrap();
    pass.started.notified().await;
    pass.permits.add_permits(1);
    handle.wait_idle().await;
    assert!(handle.is_suspended());
    let row = store.row(LoopId::Fee);
    assert!(row.last_started_at.is_some());
    assert_eq!(row.last_passed_at, None);
}

#[tokio::test]
async fn error_panic_and_later_generation_are_distinguished() {
    let store = Arc::new(MemoryStore::default());
    let pass = Arc::new(BlockingPass::new());
    let handle = one_loop(store.clone(), pass.clone()).await;
    pass.fail.store(true, Ordering::SeqCst);
    handle.request(RequestKey::from("error")).await.unwrap();
    pass.started.notified().await;
    pass.permits.add_permits(1);
    handle.wait_idle().await;
    assert!(store
        .row(LoopId::Fee)
        .last_error
        .as_deref()
        .unwrap()
        .contains("scripted pass failure"));
    assert_eq!(
        store
            .writes()
            .iter()
            .filter(|write| **write == "fail")
            .count(),
        1
    );
    pass.fail.store(false, Ordering::SeqCst);
    handle.request(RequestKey::from("recovery")).await.unwrap();
    pass.started.notified().await;
    pass.permits.add_permits(1);
    handle.wait_idle().await;
    assert!(store.row(LoopId::Fee).last_passed_at.is_some());
    pass.panic.store(true, Ordering::SeqCst);
    handle.request(RequestKey::from("panic")).await.unwrap();
    pass.started.notified().await;
    pass.permits.add_permits(1);
    handle.wait_idle().await;
    assert!(store
        .row(LoopId::Fee)
        .last_error
        .as_deref()
        .unwrap()
        .contains("panic"));
}

#[tokio::test]
async fn backpressure_write_failure_is_fail_closed_not_a_clean_admission() {
    let store = Arc::new(MemoryStore::default());
    let pass = Arc::new(BlockingPass::new());
    let handle = one_loop(store.clone(), pass.clone()).await;
    handle.request(RequestKey::from("running")).await.unwrap();
    pass.started.notified().await;
    handle.request(RequestKey::from("pending-1")).await.unwrap();
    handle.request(RequestKey::from("pending-2")).await.unwrap();
    store.fail_backpressure.store(true, Ordering::SeqCst);
    let err = handle
        .request(RequestKey::from("running"))
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("counter unavailable"));
    assert!(handle.is_suspended());
    assert_eq!(handle.abandoned_pending(), 2);
    pass.permits.add_permits(1);
    handle.wait_idle().await;
}
