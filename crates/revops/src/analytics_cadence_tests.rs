//! Task 71 / F71-R26 (RED): exact cadence and inert-until-start
//! activation for the three analytics loops.
//!
//! R27 gave the flow pass a live config and an interval to sleep on, but
//! nothing ever slept: the three passes had no scheduler at all, so on a
//! running node they ran exactly zero times unless something else poked
//! the loop handle. This module pins the schedule py actually runs
//! (cl-revenue-ops.py:3170-3225, 3462-3489, 3491-3551) rather than a
//! plausible-looking one.
//!
//! Two properties are load-bearing and each is pinned separately:
//!
//!  * **The delays are py's**, including the truncation in
//!    `int(interval * 0.2)` and the INCLUSIVE bounds of
//!    `random.randint(-j, j)`.
//!  * **The interval is read AFTER the pass completes**, because py's loop
//!    is strictly sequential: it snapshots the config at the tail, so an
//!    override applied during a pass takes effect on the very next sleep.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use revops_db::loop_health::{LoopId, WiringStatus};

use crate::analytics_cadence::{
    jitter_span, jittered_delay, one_shot_cadence, periodic_cadence, FlowCadenceActivation,
    SchedulingRng, FINANCIAL_JITTER_FRACTION, FLOW_JITTER_FRACTION,
};
use crate::analytics_passes::{
    FINANCIAL_INTERVAL_SECONDS, FINANCIAL_STARTUP_DELAY_SECONDS, FLOW_STARTUP_DELAY_SECONDS,
    STARTUP_SNAPSHOT_DELAY_SECONDS,
};
use crate::loop_health::{spawn_loop, LoopHealthPersistence, ObserverPass, RequestKey};

// ---------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------

/// The loop-health persistence the bounded owner needs, reduced to the
/// generation counter. These tests are about WHEN a pass runs; the store
/// behaviour itself is pinned in `runtime_tests`.
#[derive(Default)]
struct CountingStore {
    generation: AtomicU64,
}

impl LoopHealthPersistence for CountingStore {
    fn register<'a>(
        &'a self,
        _id: LoopId,
        _wiring: WiringStatus,
        _now: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
    fn reconcile<'a>(
        &'a self,
        _now: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<usize>> + Send + 'a>> {
        Box::pin(async { Ok(0) })
    }
    fn begin<'a>(
        &'a self,
        _id: LoopId,
        _now: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64>> + Send + 'a>> {
        Box::pin(async move { Ok(self.generation.fetch_add(1, Ordering::SeqCst) + 1) })
    }
    fn finish<'a>(
        &'a self,
        _id: LoopId,
        _generation: u64,
        _now: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
    fn fail<'a>(
        &'a self,
        _id: LoopId,
        _generation: u64,
        _now: i64,
        _error: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
    fn backpressure<'a>(
        &'a self,
        _id: LoopId,
        _coalesced: u64,
        _dropped: u64,
        _now: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
    fn suspend<'a>(
        &'a self,
        _id: LoopId,
        _now: i64,
        _reason: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
    fn is_available(&self) -> bool {
        true
    }
}

/// Records the VIRTUAL time of every pass, and optionally rewrites the
/// interval the cadence will read next. The rewrite is what distinguishes
/// "read the interval after the pass" from "read it before".
struct RecordingPass {
    start: tokio::time::Instant,
    at_secs: Mutex<Vec<u64>>,
    ran: tokio::sync::Notify,
    /// (run index, new interval) applied while the pass is running.
    rewrite: Option<(usize, Arc<AtomicU64>, u64)>,
}

impl RecordingPass {
    fn new(start: tokio::time::Instant) -> Self {
        Self {
            start,
            at_secs: Mutex::new(Vec::new()),
            ran: tokio::sync::Notify::new(),
            rewrite: None,
        }
    }

    fn rewriting(start: tokio::time::Instant, run: usize, cell: Arc<AtomicU64>, to: u64) -> Self {
        Self {
            rewrite: Some((run, cell, to)),
            ..Self::new(start)
        }
    }

    fn at_secs(&self) -> Vec<u64> {
        self.at_secs.lock().unwrap().clone()
    }

    fn count(&self) -> usize {
        self.at_secs.lock().unwrap().len()
    }

    async fn wait_for(&self, n: usize) {
        loop {
            let notified = self.ran.notified();
            if self.count() >= n {
                return;
            }
            notified.await;
        }
    }
}

impl ObserverPass for RecordingPass {
    fn run<'a>(
        &'a self,
        _key: RequestKey,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let index = {
                let mut at = self.at_secs.lock().unwrap();
                at.push((tokio::time::Instant::now() - self.start).as_secs());
                at.len() - 1
            };
            if let Some((run, cell, to)) = &self.rewrite {
                if *run == index {
                    cell.store(*to, Ordering::SeqCst);
                }
            }
            self.ran.notify_waiters();
            Ok(())
        })
    }
}

/// One recording pass behind a real bounded loop owner.
fn recorder(
    id: LoopId,
    pass: Arc<RecordingPass>,
) -> (crate::loop_health::LoopHandle, Arc<RecordingPass>) {
    let store: Arc<dyn LoopHealthPersistence> = Arc::new(CountingStore::default());
    let handle = spawn_loop(id, store, pass.clone());
    (handle, pass)
}

// ---------------------------------------------------------------------
// the jitter arithmetic
// ---------------------------------------------------------------------

/// py `jitter_seconds = int(interval * 0.2)` (cl-revenue-ops.py:3210).
/// `int()` TRUNCATES; rounding would widen the band by a second on most
/// non-round intervals, which is exactly the sort of near-miss that never
/// shows up in a range assertion.
#[test]
fn the_flow_jitter_span_is_pythons_truncated_twenty_percent() {
    assert_eq!(jitter_span(3_600, FLOW_JITTER_FRACTION), 720);
    assert_eq!(jitter_span(60, FLOW_JITTER_FRACTION), 12);
    // 899 * 0.2 == 179.8 -> 179, not 180.
    assert_eq!(jitter_span(899, FLOW_JITTER_FRACTION), 179);
    // A one-second interval has NO jitter at all in py: int(0.2) == 0.
    assert_eq!(jitter_span(1, FLOW_JITTER_FRACTION), 0);
}

/// py `jitter_seconds = int(SNAPSHOT_INTERVAL * 0.1)` (:3521).
#[test]
fn the_financial_jitter_span_is_pythons_truncated_ten_percent() {
    assert_eq!(jitter_span(86_400, FINANCIAL_JITTER_FRACTION), 8_640);
    // 86399 * 0.1 == 8639.9 -> 8639.
    assert_eq!(jitter_span(86_399, FINANCIAL_JITTER_FRACTION), 8_639);
}

/// py `random.randint(-j, j)` is INCLUSIVE at both ends, so the delay band
/// is `interval-j ..= interval+j`. A `randrange`-shaped implementation
/// would silently drop the top value, and a range assertion would never
/// notice. The band is kept small on purpose so the whole outcome set can
/// be asserted exactly rather than sampled.
#[test]
fn the_jittered_delay_covers_pythons_inclusive_symmetric_band_exactly() {
    let mut rng = SchedulingRng::seeded(0xC0FFEE);
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..10_000 {
        seen.insert(jittered_delay(10, FLOW_JITTER_FRACTION, &mut rng));
    }
    assert_eq!(
        seen,
        [8u64, 9, 10, 11, 12].into_iter().collect(),
        "int(10*0.2) == 2, so randint(-2, 2) must reach 8 and 12 inclusive"
    );
}

/// The scheduling stream must be reproducible from its OWN seed -- every
/// exact-delay assertion below depends on it -- and two different seeds
/// must not produce the same stream.
#[test]
fn the_scheduling_stream_is_reproducible_from_its_own_seed() {
    let draw = |seed: u64| {
        let mut rng = SchedulingRng::seeded(seed);
        (0..8)
            .map(|_| jittered_delay(3_600, FLOW_JITTER_FRACTION, &mut rng))
            .collect::<Vec<_>>()
    };
    assert_eq!(draw(7), draw(7));
    assert_ne!(draw(7), draw(8));
}

/// C71-8: scheduling entropy is SEPARATE and must never perturb the fee
/// controller's PyRandom. The tempting regression is the opposite of a
/// bug-shaped one -- reaching for `revops_fees::pyrand::PyRandom` in the
/// name of Python parity, because py's jitter really does come from the
/// same `random` module the fee controller uses. It cannot be shared here:
/// the fee cycle's draws are replayed and compared against Python's, so an
/// extra draw per sleep would desynchronise the entire oracle.
///
/// This pins that `SchedulingRng` is not a PyRandom wearing a different
/// name. The type-level half of the separation -- that `jittered_delay`
/// will not ACCEPT a `PyRandom` -- is a `compile_fail` doctest on the
/// function itself.
#[test]
fn the_scheduling_stream_is_not_the_fee_controllers_mt19937() {
    let mut scheduling = SchedulingRng::seeded(42);
    let scheduled: Vec<u64> = (0..16)
        .map(|_| jittered_delay(3_600, FLOW_JITTER_FRACTION, &mut scheduling))
        .collect();

    let mut py = revops_fees::pyrand::PyRandom::seed_from_u64(42);
    let span = jitter_span(3_600, FLOW_JITTER_FRACTION) as f64;
    // py `randint(-j, j)` over the same band, expressed through the one
    // primitive PyRandom exposes here.
    let pythonic: Vec<u64> = (0..16)
        .map(|_| {
            let draw = (py.random() * (2.0 * span + 1.0)).floor() - span;
            (3_600.0 + draw) as u64
        })
        .collect();
    assert_ne!(
        scheduled, pythonic,
        "the scheduling stream must not be drawn from the fee controller's generator"
    );
}

// ---------------------------------------------------------------------
// flow cadence
// ---------------------------------------------------------------------

/// py `flow_analysis_loop`: `shutdown_event.wait(30)` before the first
/// pass, then `interval + randint(-j, j)` between passes
/// (cl-revenue-ops.py:3173, 3209-3215).
///
/// The expected delays are recomputed here from the SAME seed rather than
/// range-checked, so dropping the jitter, dropping the startup stagger, or
/// sleeping the interval before the first pass all fail loudly.
#[tokio::test(start_paused = true)]
async fn the_flow_cadence_waits_thirty_seconds_then_sleeps_the_jittered_interval() {
    let start = tokio::time::Instant::now();
    let (handle, pass) = recorder(LoopId::FlowAnalysis, Arc::new(RecordingPass::new(start)));
    let interval = Arc::new(AtomicU64::new(600));
    let read = interval.clone();
    tokio::spawn(periodic_cadence(
        handle,
        "flow-analysis",
        FLOW_STARTUP_DELAY_SECONDS,
        FLOW_JITTER_FRACTION,
        SchedulingRng::seeded(7),
        move || read.load(Ordering::SeqCst),
    ));

    pass.wait_for(3).await;

    let mut expected = SchedulingRng::seeded(7);
    let first = jittered_delay(600, FLOW_JITTER_FRACTION, &mut expected);
    let second = jittered_delay(600, FLOW_JITTER_FRACTION, &mut expected);
    assert_eq!(
        pass.at_secs()[..3],
        [30, 30 + first, 30 + first + second],
        "first pass at the 30s stagger, then py's jittered interval between passes"
    );
}

/// py reads the interval from a config snapshot taken AFTER the pass
/// returns (the M-3 fix at cl-revenue-ops.py:3207-3209), so an operator's
/// `revenue-config set flow_interval` applied while a pass is running
/// governs that pass's own sleep -- not the one after it.
///
/// The cadence therefore has to wait for the pass to complete before
/// reading `interval_secs()`. Reading it at request time instead is
/// invisible in a steady state and only diverges here.
#[tokio::test(start_paused = true)]
async fn the_flow_cadence_reads_the_interval_after_the_pass_not_before() {
    let start = tokio::time::Instant::now();
    let interval = Arc::new(AtomicU64::new(600));
    let (handle, pass) = recorder(
        LoopId::FlowAnalysis,
        Arc::new(RecordingPass::rewriting(start, 0, interval.clone(), 1_200)),
    );
    let read = interval.clone();
    tokio::spawn(periodic_cadence(
        handle,
        "flow-analysis",
        FLOW_STARTUP_DELAY_SECONDS,
        FLOW_JITTER_FRACTION,
        SchedulingRng::seeded(7),
        move || read.load(Ordering::SeqCst),
    ));

    pass.wait_for(2).await;

    let mut expected = SchedulingRng::seeded(7);
    let after = jittered_delay(1_200, FLOW_JITTER_FRACTION, &mut expected);
    assert_eq!(
        pass.at_secs()[1],
        30 + after,
        "the sleep after a pass must use the interval that pass left behind"
    );
}

// ---------------------------------------------------------------------
// startup snapshot
// ---------------------------------------------------------------------

/// py `snapshot_peers_delayed`: one 60s wait, one run, then the thread
/// EXITS (cl-revenue-ops.py:3462-3489). A one-shot that quietly became
/// periodic would keep re-recording startup snapshot events for the life
/// of the process.
#[tokio::test(start_paused = true)]
async fn the_startup_snapshot_runs_once_at_sixty_seconds_and_never_again() {
    let start = tokio::time::Instant::now();
    let (handle, pass) = recorder(LoopId::StartupSnapshot, Arc::new(RecordingPass::new(start)));
    tokio::spawn(one_shot_cadence(
        handle,
        "startup-snapshot",
        STARTUP_SNAPSHOT_DELAY_SECONDS,
    ));

    pass.wait_for(1).await;
    assert_eq!(pass.at_secs(), [60]);

    tokio::time::advance(Duration::from_secs(86_400 * 7)).await;
    assert_eq!(pass.count(), 1, "the startup snapshot is a one-shot");
}

// ---------------------------------------------------------------------
// financial snapshot
// ---------------------------------------------------------------------

/// py `financial_snapshot_loop`: `wait(300)`, snapshot, then a sleep-first
/// tail of `86400 + randint(-8640, 8640)` (cl-revenue-ops.py:3501-3528).
#[tokio::test(start_paused = true)]
async fn the_financial_cadence_runs_at_five_minutes_then_daily_with_ten_percent_jitter() {
    let start = tokio::time::Instant::now();
    let (handle, pass) = recorder(
        LoopId::FinancialSnapshot,
        Arc::new(RecordingPass::new(start)),
    );
    tokio::spawn(periodic_cadence(
        handle,
        "financial-snapshot",
        FINANCIAL_STARTUP_DELAY_SECONDS,
        FINANCIAL_JITTER_FRACTION,
        SchedulingRng::seeded(11),
        || FINANCIAL_INTERVAL_SECONDS as u64,
    ));

    pass.wait_for(2).await;

    let mut expected = SchedulingRng::seeded(11);
    let daily = jittered_delay(86_400, FINANCIAL_JITTER_FRACTION, &mut expected);
    assert_eq!(pass.at_secs()[..2], [300, 300 + daily]);
    assert!(
        (77_760..=95_040).contains(&daily),
        "86400 +/- 10% is 77760..=95040, got {daily}"
    );
}

// ---------------------------------------------------------------------
// activation ordering
// ---------------------------------------------------------------------

/// C71-9's production-composition proof, part one: that `main` actually
/// composes all three passes and starts their cadences AFTER the plugin is
/// serving.
///
/// `main.rs` is a binary target, so no test can import it -- which is
/// exactly how R16's original defect survived: complete, tested passes
/// with no production caller. A source-level assertion is the only
/// available proof of the ordering, and this codebase already uses the
/// idiom for the observation-only boundary (`tests/flow_owner.rs:212`).
///
/// The ordering is load-bearing, not stylistic. py starts its analytics
/// threads at the very end of `init` (cl-revenue-ops.py:3588-3600). A
/// cadence activated during composition instead could fire its first pass
/// while `configured.start()` is still completing lightningd's init
/// handshake, against a socket that is not answering RPCs yet.
#[test]
fn main_composes_all_three_analytics_passes_and_activates_them_after_start() {
    let source =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/main.rs")).unwrap();

    for construction in [
        "FlowAnalysisPass::live(",
        "StartupSnapshotPass::live(",
        "FinancialSnapshotPass::live(",
        "with_flow_analysis(",
        "with_startup_snapshot(",
        "with_financial_snapshot(",
    ] {
        assert!(
            source.contains(construction),
            "main.rs must compose the analytics owners (missing `{construction}`)"
        );
    }

    let started = source
        .find("configured.start(state)")
        .expect("main.rs must start the plugin");
    for activation in [
        "flow_cadence.activate()",
        "startup_snapshot_cadence.activate()",
        "financial_cadence.activate()",
    ] {
        let at = source
            .find(activation)
            .unwrap_or_else(|| panic!("main.rs must activate the cadence (`{activation}`)"));
        assert!(
            at > started,
            "`{activation}` must come AFTER configured.start(state), not during composition"
        );
    }
}

/// C71-9's production-composition proof, part two: the three analytics
/// loops compose and register `Ready` under a PASSIVE observer, not only
/// under autonomous shadow.
///
/// `ObserverRuntime::start` refuses the fee and LN+ passes outside
/// autonomous shadow because those carry action capability. These three do
/// not: read-only RPC, frozen kernels, writes confined to the Rust-owned
/// observer store. Composing them inside the `if autonomous_shadow` block
/// would compile, pass every other test, and silently leave a passive node
/// with no flow analysis at all -- reporting `NotWired` forever, which is
/// the Task 67 false-health shape this work exists to remove.
#[tokio::test]
async fn the_analytics_loops_compose_under_a_passive_observer() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    let observer = revops_db::owner::spawn_read_write(&dir.path().join("observer.sqlite3"))
        .await
        .expect("spawn observer db");
    let store = Arc::new(crate::loop_health::LoopHealthStore::new(
        observer.clone(),
        "boot-under-test".to_string(),
    ));

    let passes = crate::runtime::ObserverPassSet::empty()
        .with_flow_analysis(Arc::new(
            crate::analytics_passes::FlowAnalysisPass::for_tests(
                socket.clone(),
                observer.clone(),
                "boot-under-test".to_string(),
                crate::flow_config::FlowConfigSources {
                    db_overrides: Ok(std::collections::BTreeMap::new()),
                    listconfigs: std::collections::BTreeMap::new(),
                    listconfigs_freshness: crate::config_resolve::SnapshotFreshness::Fresh,
                },
            ),
        ))
        .with_startup_snapshot(Arc::new(
            crate::analytics_passes::StartupSnapshotPass::live(socket.clone(), observer.clone()),
        ))
        .with_financial_snapshot(Arc::new(
            crate::analytics_passes::FinancialSnapshotPass::for_tests(
                observer.clone(),
                "boot-under-test".to_string(),
                Err("not exercised here".to_string()),
                Err("not exercised here".to_string()),
                1_800_000_000,
            ),
        ));

    let runtime = crate::runtime::ObserverRuntime::start(
        crate::fee_mode::ObserverMode::for_tests(false),
        store,
        passes,
    )
    .await
    .expect("a passive observer must still compose the observation-only analytics loops");

    let rows = observer.list_loop_health().await.unwrap();
    for id in [
        LoopId::FlowAnalysis,
        LoopId::StartupSnapshot,
        LoopId::FinancialSnapshot,
    ] {
        assert!(
            runtime.handle(id).is_some(),
            "{id:?} must be wired under a passive observer"
        );
        let row = rows.iter().find(|r| r.loop_id == id).unwrap();
        assert_eq!(
            row.wiring_status,
            WiringStatus::Ready,
            "{id:?} must register Ready under a passive observer"
        );
    }
}

/// C71-8: "activate only after plugin/runtime start". Construction must be
/// INERT -- if `new()` spawned the timer, the first flow pass could land
/// before `plugin.start()` returned, i.e. before the RPC surface, the
/// option cache, or the shared state the pass reads even exist.
///
/// This is the ONLY cadence test that builds a real `ObserverHandle`, and
/// that is why it drives the clock with explicit `advance()` calls instead
/// of letting paused time auto-advance. The observer store is a
/// `spawn_blocking` actor parked on `blocking_recv()` for the lifetime of
/// the handle (`revops-db/src/owner.rs:2307`), so the runtime is never
/// idle, so tokio never auto-advances -- a sleep of 30 virtual seconds
/// then waits forever at zero CPU. Explicit advances are also the sharper
/// assertion: they pin that NOTHING ran across two full days rather than
/// merely that nothing ran yet.
#[tokio::test(start_paused = true)]
async fn constructing_the_flow_activation_starts_no_timer() {
    let dir = tempfile::tempdir().unwrap();
    let observer = revops_db::owner::spawn_read_write(&dir.path().join("observer.sqlite3"))
        .await
        .expect("spawn observer db");
    let start = tokio::time::Instant::now();
    let (handle, pass) = recorder(LoopId::FlowAnalysis, Arc::new(RecordingPass::new(start)));
    let flow = Arc::new(crate::analytics_passes::FlowAnalysisPass::for_tests(
        dir.path().join("no-such-lightning-rpc"),
        observer,
        "boot-under-test".to_string(),
        crate::flow_config::FlowConfigSources {
            db_overrides: Ok(std::collections::BTreeMap::new()),
            listconfigs: std::collections::BTreeMap::new(),
            listconfigs_freshness: crate::config_resolve::SnapshotFreshness::Fresh,
        },
    ));

    let activation = FlowCadenceActivation::new(handle, flow);
    tokio::time::advance(Duration::from_secs(86_400 * 2)).await;
    assert_eq!(
        pass.count(),
        0,
        "an unactivated cadence must never reach the loop"
    );

    activation.activate();
    // `activate()` only SPAWNS; the sleep future does not exist -- and so
    // has not registered a deadline with the time driver -- until that
    // task is first polled. Advancing the clock before that poll makes the
    // sleep compute its deadline from the ALREADY-advanced clock, and with
    // the observer store's blocking actor suppressing auto-advance there
    // is nothing left to move time again: the test parks forever at zero
    // CPU. One yield lets the cadence register against the pre-advance
    // clock. Real time never needs this, which is precisely why it is a
    // test-harness trap rather than a cadence defect.
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(29)).await;
    assert_eq!(
        pass.count(),
        0,
        "py waits the full 30s stagger before the first flow pass"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    pass.wait_for(1).await;
    assert_eq!(
        pass.at_secs(),
        [86_400 * 2 + 30],
        "activation starts py's 30s startup stagger, measured from activation"
    );
}
