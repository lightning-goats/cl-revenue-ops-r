use anyhow::{Context, Result};
use revops_db::loop_health::{LoopId, WiringStatus};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

pub const MAX_PENDING_KEYS: usize = 8;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RequestKey(String);
impl From<&str> for RequestKey {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}
impl From<String> for RequestKey {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Admission {
    Enqueued,
    Coalesced,
    Dropped,
}

pub trait ObserverPass: Send + Sync + 'static {
    fn run<'a>(&'a self, key: RequestKey) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

pub trait LoopHealthPersistence: Send + Sync + 'static {
    fn register<'a>(
        &'a self,
        id: LoopId,
        wiring: WiringStatus,
        now: i64,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
    fn reconcile<'a>(
        &'a self,
        now: i64,
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send + 'a>>;
    fn begin<'a>(
        &'a self,
        id: LoopId,
        now: i64,
    ) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + 'a>>;
    fn finish<'a>(
        &'a self,
        id: LoopId,
        generation: u64,
        now: i64,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
    fn fail<'a>(
        &'a self,
        id: LoopId,
        generation: u64,
        now: i64,
        error: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
    fn backpressure<'a>(
        &'a self,
        id: LoopId,
        coalesced: u64,
        dropped: u64,
        now: i64,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

#[derive(Clone)]
pub struct LoopHealthStore {
    handle: revops_db::owner::ObserverHandle,
}
impl LoopHealthStore {
    pub fn new(handle: revops_db::owner::ObserverHandle) -> Self {
        Self { handle }
    }
}

impl LoopHealthPersistence for LoopHealthStore {
    fn register<'a>(
        &'a self,
        id: LoopId,
        wiring: WiringStatus,
        now: i64,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { self.handle.register_loop(id, wiring, now).await })
    }
    fn reconcile<'a>(
        &'a self,
        now: i64,
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send + 'a>> {
        Box::pin(async move { self.handle.reconcile_incomplete_loops(now).await })
    }
    fn begin<'a>(
        &'a self,
        id: LoopId,
        now: i64,
    ) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + 'a>> {
        Box::pin(async move { self.handle.begin_loop_pass(id, now).await })
    }
    fn finish<'a>(
        &'a self,
        id: LoopId,
        generation: u64,
        now: i64,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { self.handle.finish_loop_pass(id, generation, now).await })
    }
    fn fail<'a>(
        &'a self,
        id: LoopId,
        generation: u64,
        now: i64,
        error: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.handle
                .fail_loop_pass(id, generation, now, error.to_string())
                .await
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
            self.handle
                .increment_loop_backpressure(id, coalesced, dropped, now)
                .await
        })
    }
}

#[derive(Default)]
struct Ingress {
    running: Option<RequestKey>,
    pending: VecDeque<RequestKey>,
}

struct Shared {
    id: LoopId,
    store: Arc<dyn LoopHealthPersistence>,
    ingress: Mutex<Ingress>,
    suspended: AtomicBool,
    abandoned_pending: std::sync::atomic::AtomicU64,
    work: Notify,
    idle: Notify,
}

#[derive(Clone)]
pub struct LoopHandle {
    shared: Arc<Shared>,
}

impl LoopHandle {
    pub async fn request(&self, key: RequestKey) -> Result<Admission> {
        enum Decision {
            Enqueued,
            Backpressure(Admission, u64, u64),
        }
        let decision = {
            let mut ingress = self.shared.ingress.lock().expect("loop ingress poisoned");
            if self.shared.suspended.load(Ordering::SeqCst) {
                Decision::Backpressure(Admission::Dropped, 0, 1)
            } else if ingress.running.as_ref() == Some(&key) || ingress.pending.contains(&key) {
                Decision::Backpressure(Admission::Coalesced, 1, 0)
            } else if ingress.pending.len() >= MAX_PENDING_KEYS {
                Decision::Backpressure(Admission::Dropped, 0, 1)
            } else {
                ingress.pending.push_back(key);
                Decision::Enqueued
            }
        };
        match decision {
            Decision::Enqueued => {
                self.shared.work.notify_one();
                Ok(Admission::Enqueued)
            }
            Decision::Backpressure(admission, coalesced, dropped) => {
                if let Err(error) = self
                    .shared
                    .store
                    .backpressure(self.shared.id, coalesced, dropped, crate::now_unix())
                    .await
                {
                    let abandoned = self.suspend();
                    if abandoned > 0 {
                        if let Err(drop_error) = self
                            .shared
                            .store
                            .backpressure(self.shared.id, 0, abandoned, crate::now_unix())
                            .await
                        {
                            eprintln!("revops: {:?} loop could not persist {abandoned} abandoned pending keys after suspension: {drop_error:#}", self.shared.id);
                        }
                    }
                    return Err(error)
                        .context("persist loop backpressure before reporting admission");
                }
                Ok(admission)
            }
        }
    }

    pub fn is_suspended(&self) -> bool {
        self.shared.suspended.load(Ordering::SeqCst)
    }
    pub fn abandoned_pending(&self) -> u64 {
        self.shared.abandoned_pending.load(Ordering::SeqCst)
    }

    fn suspend(&self) -> u64 {
        suspend_shared(&self.shared)
    }

    pub async fn wait_idle(&self) {
        loop {
            let notified = self.shared.idle.notified();
            let idle = {
                let ingress = self.shared.ingress.lock().expect("loop ingress poisoned");
                ingress.running.is_none() && ingress.pending.is_empty()
            };
            if idle {
                return;
            }
            notified.await;
        }
    }
}

pub fn spawn_loop(
    id: LoopId,
    store: Arc<dyn LoopHealthPersistence>,
    pass: Arc<dyn ObserverPass>,
) -> LoopHandle {
    let shared = Arc::new(Shared {
        id,
        store,
        ingress: Mutex::new(Ingress::default()),
        suspended: AtomicBool::new(false),
        abandoned_pending: std::sync::atomic::AtomicU64::new(0),
        work: Notify::new(),
        idle: Notify::new(),
    });
    let handle = LoopHandle {
        shared: shared.clone(),
    };
    tokio::spawn(owner_task(shared, pass));
    handle
}

async fn owner_task(shared: Arc<Shared>, pass: Arc<dyn ObserverPass>) {
    loop {
        let key = loop {
            if shared.suspended.load(Ordering::SeqCst) {
                return;
            }
            if let Some(key) = {
                let mut ingress = shared.ingress.lock().expect("loop ingress poisoned");
                let key = ingress.pending.pop_front();
                if let Some(ref key) = key {
                    ingress.running = Some(key.clone());
                }
                key
            } {
                break key;
            }
            shared.idle.notify_waiters();
            shared.work.notified().await;
        };

        let generation = match shared.store.begin(shared.id, crate::now_unix()).await {
            Ok(generation) => generation,
            Err(error) => {
                eprintln!(
                    "revops: {:?} loop begin persistence failed: {error:#}; suspending",
                    shared.id
                );
                let abandoned = suspend_shared(&shared) + 1;
                shared.abandoned_pending.fetch_add(1, Ordering::SeqCst);
                persist_abandoned(&shared, abandoned).await;
                continue;
            }
        };
        let child_pass = pass.clone();
        let outcome = match tokio::spawn(async move { child_pass.run(key).await }).await {
            Ok(result) => result,
            Err(join) => Err(anyhow::anyhow!("observer pass panic: {join}")),
        };
        let terminal = match outcome {
            Ok(()) => {
                shared
                    .store
                    .finish(shared.id, generation, crate::now_unix())
                    .await
            }
            Err(error) => {
                shared
                    .store
                    .fail(
                        shared.id,
                        generation,
                        crate::now_unix(),
                        &format!("{error:#}"),
                    )
                    .await
            }
        };
        if let Err(error) = terminal {
            eprintln!(
                "revops: {:?} loop terminal persistence failed: {error:#}; suspending",
                shared.id
            );
            let abandoned = suspend_shared(&shared);
            persist_abandoned(&shared, abandoned).await;
        } else {
            shared
                .ingress
                .lock()
                .expect("loop ingress poisoned")
                .running = None;
            shared.idle.notify_waiters();
        }
    }
}

fn suspend_shared(shared: &Shared) -> u64 {
    shared.suspended.store(true, Ordering::SeqCst);
    let mut ingress = shared.ingress.lock().expect("loop ingress poisoned");
    let abandoned = ingress.pending.len() as u64;
    ingress.running = None;
    ingress.pending.clear();
    shared
        .abandoned_pending
        .fetch_add(abandoned, Ordering::SeqCst);
    shared.idle.notify_waiters();
    shared.work.notify_waiters();
    abandoned
}

async fn persist_abandoned(shared: &Shared, abandoned: u64) {
    if abandoned > 0 {
        if let Err(error) = shared
            .store
            .backpressure(shared.id, 0, abandoned, crate::now_unix())
            .await
        {
            eprintln!("revops: {:?} loop could not persist {abandoned} abandoned pending keys after suspension: {error:#}", shared.id);
        }
    }
}
