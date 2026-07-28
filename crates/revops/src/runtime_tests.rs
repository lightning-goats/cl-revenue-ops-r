use crate::loop_health::{Admission, LoopHandle, LoopHealthPersistence, ObserverPass, RequestKey};
use crate::runtime::{ObserverRuntime, REQUIRED_LOOPS};
use anyhow::{anyhow, Result};
use revops_db::loop_health::{LoopHealthRow, LoopId, RuntimeStatus, WiringStatus};
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
    fail_suspend_attempts: AtomicUsize,
    suspend_attempts: AtomicUsize,
    actor_unavailable: AtomicBool,
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
    fn suspend<'a>(
        &'a self,
        id: LoopId,
        now: i64,
        reason: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.writes.lock().unwrap().push("suspend");
            self.suspend_attempts.fetch_add(1, Ordering::SeqCst);
            if self
                .fail_suspend_attempts
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(anyhow!("suspension marker unavailable"));
            }
            let mut rows = self.rows.lock().unwrap();
            let row = rows.get_mut(&id).unwrap();
            row.runtime_status = RuntimeStatus::Suspended;
            row.last_suspended_at = Some(now);
            row.last_suspension_reason = Some(reason.to_string());
            row.updated_at = now;
            Ok(())
        })
    }
    fn is_available(&self) -> bool {
        !self.actor_unavailable.load(Ordering::SeqCst)
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
    ObserverRuntime::start_for_tests(store, passes)
        .await
        .unwrap()
        .handle(LoopId::Fee)
        .unwrap()
}

#[tokio::test]
async fn registers_exact_five_identities_without_noop_owners() {
    let store = Arc::new(MemoryStore::default());
    let runtime = ObserverRuntime::start_for_tests(store.clone(), BTreeMap::new())
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
    let runtime = ObserverRuntime::start_for_tests(store.clone(), passes)
        .await
        .unwrap();
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
    assert_eq!(
        store.row(LoopId::Fee).runtime_status,
        RuntimeStatus::Suspended
    );
    assert!(store
        .row(LoopId::Fee)
        .last_suspension_reason
        .as_deref()
        .unwrap()
        .contains("begin"));
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
    assert_eq!(row.runtime_status, RuntimeStatus::Suspended);
    assert!(row
        .last_suspension_reason
        .as_deref()
        .unwrap()
        .contains("terminal"));
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

#[tokio::test]
async fn transient_backpressure_failure_retries_suspension_and_late_finish_cannot_mask_it() {
    let store = Arc::new(MemoryStore::default());
    let pass = Arc::new(BlockingPass::new());
    let handle = one_loop(store.clone(), pass.clone()).await;
    handle.request(RequestKey::from("running")).await.unwrap();
    pass.started.notified().await;
    store.fail_backpressure.store(true, Ordering::SeqCst);
    store.fail_suspend_attempts.store(2, Ordering::SeqCst);
    let request_handle = handle.clone();
    let request = tokio::spawn(async move {
        request_handle
            .request(RequestKey::from("running"))
            .await
            .unwrap_err()
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while store.suspend_attempts.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("durable suspension write must be attempted");
    pass.permits.add_permits(1);
    let error = request.await.unwrap();
    assert!(format!("{error:#}").contains("counter unavailable"));
    handle.wait_idle().await;
    let row = store.row(LoopId::Fee);
    assert_eq!(store.suspend_attempts.load(Ordering::SeqCst), 3);
    assert_eq!(row.runtime_status, RuntimeStatus::Suspended);
    assert!(
        row.last_passed_at.is_some(),
        "the concurrent pass really finished"
    );
    assert!(row
        .last_suspension_reason
        .as_deref()
        .unwrap()
        .contains("backpressure"));
}

#[tokio::test]
async fn suspension_retry_stops_only_when_store_actor_is_provably_unavailable() {
    let store = Arc::new(MemoryStore::default());
    let pass = Arc::new(BlockingPass::new());
    store.fail_begin.store(true, Ordering::SeqCst);
    store.fail_suspend_attempts.store(10, Ordering::SeqCst);
    store.actor_unavailable.store(true, Ordering::SeqCst);
    let handle = one_loop(store.clone(), pass.clone()).await;
    handle
        .request(RequestKey::from("never-runs"))
        .await
        .unwrap();
    handle.wait_idle().await;
    assert!(handle.is_suspended());
    assert_eq!(pass.calls.load(Ordering::SeqCst), 0);
    assert_eq!(store.suspend_attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn fee_cadence_makes_zero_requests_until_explicit_post_start_activation() {
    use crate::fee_mode::{validate_fee_mode, AuthorityPlan, ModeFlags};
    use crate::fee_scheduler::{FeeCadenceActivation, FeeObserverPass, SchedulerIngress};
    use crate::runtime::ObserverPassSet;
    use revops_db::fee_runway::FeeStateSnapshot;

    let mode = validate_fee_mode(
        ModeFlags {
            observer: true,
            fee_dryrun: true,
            fee_broadcast: false,
            fee_stateful_shadow: true,
        },
        None,
        &FeeStateSnapshot::default(),
        None,
    )
    .unwrap();
    let observer_mode = match mode.into_authority_plan(|_| -> () {
        panic!("observer cadence construction touched action factory")
    }) {
        AuthorityPlan::Observer(token) => token,
        AuthorityPlan::Live(()) => unreachable!(),
    };
    let store = Arc::new(MemoryStore::default());
    let (scheduler_tx, _owner_rx) = SchedulerIngress::bounded_channel(1);
    let pass = Arc::new(FeeObserverPass::new(
        std::path::PathBuf::from("/nonexistent/post-start-cadence-test-rpc"),
        None,
        crate::config_resolve::PythonOptionCache::empty(),
        scheduler_tx,
        1,
    ));
    let runtime = ObserverRuntime::start(
        observer_mode,
        store.clone(),
        ObserverPassSet::empty().with_fee(pass.clone()),
    )
    .await
    .unwrap();
    let activation = FeeCadenceActivation::new(runtime.handle(LoopId::Fee).unwrap(), pass, 0);

    tokio::time::advance(std::time::Duration::from_secs(300)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        store.row(LoopId::Fee).generation,
        0,
        "constructing the runtime and cadence handle must remain inert before plugin start"
    );

    activation.activate();
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(store.row(LoopId::Fee).generation, 1);
}

#[tokio::test]
async fn passive_observer_token_rejects_a_fee_pass() {
    use crate::fee_mode::{validate_fee_mode, AuthorityPlan, ModeFlags};
    use crate::fee_scheduler::{FeeObserverPass, SchedulerIngress};
    use crate::runtime::ObserverPassSet;
    use revops_db::fee_runway::FeeStateSnapshot;

    let mode = validate_fee_mode(
        ModeFlags {
            observer: true,
            fee_dryrun: false,
            fee_broadcast: false,
            fee_stateful_shadow: false,
        },
        None,
        &FeeStateSnapshot::default(),
        None,
    )
    .unwrap();
    let observer_mode = match mode.into_authority_plan(|_| -> () {
        panic!("passive observer construction touched action factory")
    }) {
        AuthorityPlan::Observer(token) => token,
        AuthorityPlan::Live(()) => unreachable!(),
    };
    let (scheduler_tx, _owner_rx) = SchedulerIngress::bounded_channel(1);
    let pass = Arc::new(FeeObserverPass::new(
        std::path::PathBuf::from("/nonexistent/passive-fee-pass-test-rpc"),
        None,
        crate::config_resolve::PythonOptionCache::empty(),
        scheduler_tx,
        60,
    ));
    let store = Arc::new(MemoryStore::default());
    let result = ObserverRuntime::start(
        observer_mode,
        store.clone(),
        ObserverPassSet::empty().with_fee(pass),
    )
    .await;
    let error = match result {
        Ok(_) => panic!("passive observer accepted an autonomous fee pass"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("passive observer cannot start the autonomous fee pass"));
    assert!(
        store.writes().is_empty(),
        "mode/pass-set refusal must precede registration and reconciliation"
    );
}
