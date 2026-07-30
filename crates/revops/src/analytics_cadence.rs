//! Task 71 / F71-R26: the cadence and activation for the three analytics
//! loops.
//!
//! R16 gave FlowAnalysis / StartupSnapshot / FinancialSnapshot real
//! observer passes and R27 gave the flow pass a live config, but nothing
//! ever CALLED them: the bounded loop owners sat waiting for a request
//! that no timer ever sent. A loop that is wired but never triggered
//! reports `Ready` and never fails, which is the same false-health shape
//! Task 67 set out to remove, one level further out.
//!
//! Three properties are load-bearing here.
//!
//! **The delays are Python's, exactly.** py's flow loop staggers 30s,
//! then sleeps `interval + randint(-int(interval*0.2), +int(...))`; the
//! startup snapshot is a single 60s one-shot; the financial loop staggers
//! 300s then sleeps `86400 +/- 10%` (cl-revenue-ops.py:3173, 3209-3215,
//! 3469-3473, 3501, 3521-3522). The truncation and the INCLUSIVE randint
//! bounds are both reproduced rather than approximated.
//!
//! **The interval is read after the pass, not before.** py's loops are
//! strictly sequential: the pass runs to completion, THEN the config is
//! snapshotted for the sleep (the M-3 fix at :3207-3209). The cadence
//! therefore awaits `wait_idle()` before it asks the pass what interval it
//! resolved, so an override applied during a pass governs that pass's own
//! sleep.
//!
//! **Scheduling entropy is separate from the fee controller's**
//! (C71-8). py draws jitter from the same module-level `random` the fee
//! controller uses, and that one detail is deliberately NOT ported: the
//! fee cycle's draws are replayed against Python's oracle, so one extra
//! draw per sleep -- from a background thread, at a nondeterministic point
//! -- would desynchronise every downstream decision. [`SchedulingRng`] is
//! its own generator, and [`jittered_delay`] will not accept a `PyRandom`.

use std::sync::Arc;
use std::time::Duration;

use crate::analytics_passes::{
    FlowAnalysisPass, FINANCIAL_INTERVAL_SECONDS, FINANCIAL_STARTUP_DELAY_SECONDS,
    FLOW_STARTUP_DELAY_SECONDS, STARTUP_SNAPSHOT_DELAY_SECONDS,
};
use crate::loop_health::{Admission, LoopHandle, RequestKey};

/// py `jitter_seconds = int(interval * 0.2)` (cl-revenue-ops.py:3210).
pub const FLOW_JITTER_FRACTION: f64 = 0.2;

/// py `jitter_seconds = int(SNAPSHOT_INTERVAL * 0.1)`
/// (cl-revenue-ops.py:3521).
pub const FINANCIAL_JITTER_FRACTION: f64 = 0.1;

/// The single coalescing key every cadence uses, matching the fee and LN+
/// triggers: two overlapping ticks are the same work, not two jobs.
const REQUEST_KEY: &str = "fixed_interval";

// =====================================================================
// scheduling entropy
// =====================================================================

/// The scheduling-only random stream.
///
/// SplitMix64 over a per-process seed. Deliberately not a
/// `revops_fees::pyrand::PyRandom`: see the module doc: sharing the fee
/// controller's generator would perturb a stream that is replayed against
/// Python's, and the divergence would appear as unexplained fee decisions
/// rather than as a scheduling bug.
///
/// Jitter exists to spread load, so an independent uniform stream is
/// behaviourally equivalent to py's for its actual purpose. What is NOT
/// optional is the shape of the draw, which [`jittered_delay`] reproduces
/// exactly.
pub struct SchedulingRng {
    state: u64,
}

impl SchedulingRng {
    /// Seeded from the process's own entropy. `RandomState`'s keys are
    /// OS-seeded and differ per instance, so two loops in one process do
    /// not share a phase -- which is the whole point of jitter.
    pub fn from_process_entropy() -> Self {
        use std::hash::{BuildHasher, Hasher};
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u64(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
        );
        hasher.write_u32(std::process::id());
        Self::seeded(hasher.finish())
    }

    /// A pinned stream, so a cadence's exact delay sequence is assertable.
    pub fn seeded(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// py `random.randint(-span, span)`: uniform and INCLUSIVE at both
    /// ends. Rejection-sampled rather than `% n`, so the ends are not
    /// quietly under-represented.
    fn randint_symmetric(&mut self, span: u64) -> i64 {
        if span == 0 {
            return 0;
        }
        let n = span.saturating_mul(2).saturating_add(1);
        // The largest multiple of `n` that fits; draws at or above it
        // would bias the low end of the band.
        let zone = u64::MAX - (u64::MAX % n);
        let mut draw = self.next_u64();
        while draw >= zone {
            draw = self.next_u64();
        }
        (draw % n) as i64 - span as i64
    }
}

/// py `jitter_seconds = int(interval * fraction)` -- `int()` TRUNCATES.
pub fn jitter_span(interval_secs: u64, fraction: f64) -> u64 {
    (interval_secs as f64 * fraction) as u64
}

/// py `sleep_time = interval + random.randint(-jitter, jitter)`.
///
/// The fee controller's generator is not accepted here, by construction
/// (C71-8):
///
/// ```compile_fail,E0308
/// let mut fee_rng = revops_fees::pyrand::PyRandom::seed_from_u64(1);
/// revops::analytics_cadence::jittered_delay(3600, 0.2, &mut fee_rng);
/// ```
///
/// ```
/// let mut rng = revops::analytics_cadence::SchedulingRng::seeded(1);
/// let delay = revops::analytics_cadence::jittered_delay(3600, 0.2, &mut rng);
/// assert!((2880..=4320).contains(&delay));
/// ```
pub fn jittered_delay(interval_secs: u64, fraction: f64, rng: &mut SchedulingRng) -> u64 {
    let span = jitter_span(interval_secs, fraction);
    let draw = rng.randint_symmetric(span);
    (interval_secs as i64).saturating_add(draw).max(0) as u64
}

// =====================================================================
// drivers
// =====================================================================

/// Enqueue one tick. `false` means the trigger must exit: a request that
/// cannot be recorded at all is the same condition the fee and LN+
/// triggers already treat as terminal, because a loop whose backpressure
/// is unrecordable can no longer be judged from its own health row.
async fn tick(handle: &LoopHandle, label: &'static str) -> bool {
    match handle.request(RequestKey::from(REQUEST_KEY)).await {
        Ok(Admission::Enqueued | Admission::Coalesced) => true,
        Ok(Admission::Dropped) => {
            eprintln!("revops: {label} loop request dropped by bounded runtime");
            true
        }
        Err(error) => {
            eprintln!(
                "revops: {label} loop request persistence failed: {error:#}; trigger exiting"
            );
            false
        }
    }
}

/// py's `wait(startup); while ...: run; sleep(interval +/- jitter)`.
///
/// `interval_secs` is read AFTER `wait_idle()` so it observes whatever the
/// pass that just ran resolved -- py snapshots its config at the same
/// point, for the same reason.
pub(crate) async fn periodic_cadence<F>(
    handle: LoopHandle,
    label: &'static str,
    startup_delay_secs: u64,
    jitter_fraction: f64,
    mut rng: SchedulingRng,
    interval_secs: F,
) where
    F: Fn() -> u64 + Send + 'static,
{
    let mut delay = startup_delay_secs;
    loop {
        tokio::time::sleep(Duration::from_secs(delay)).await;
        if !tick(&handle, label).await {
            return;
        }
        handle.wait_idle().await;
        delay = jittered_delay(interval_secs(), jitter_fraction, &mut rng);
    }
}

/// py `snapshot_peers_delayed`: one wait, one run, then the thread exits.
pub(crate) async fn one_shot_cadence(handle: LoopHandle, label: &'static str, delay_secs: u64) {
    tokio::time::sleep(Duration::from_secs(delay_secs)).await;
    tick(&handle, label).await;
}

// =====================================================================
// activations
// =====================================================================

/// Inert until [`activate`](FlowCadenceActivation::activate), mirroring
/// `FeeCadenceActivation` and `LnPlusCadenceActivation`: composition
/// builds these, and `main` starts them only after `plugin.start()` has
/// returned.
pub struct FlowCadenceActivation {
    handle: LoopHandle,
    pass: Arc<FlowAnalysisPass>,
}

impl FlowCadenceActivation {
    pub fn new(handle: LoopHandle, pass: Arc<FlowAnalysisPass>) -> Self {
        Self { handle, pass }
    }

    pub fn activate(self) {
        let Self { handle, pass } = self;
        // `interval_secs()` already applies py's `max(60, flow_interval)`
        // floor and returns what the LAST pass resolved -- the flooring
        // lives with the pass so the cadence cannot drift from it.
        tokio::spawn(periodic_cadence(
            handle,
            "flow-analysis",
            FLOW_STARTUP_DELAY_SECONDS,
            FLOW_JITTER_FRACTION,
            SchedulingRng::from_process_entropy(),
            move || pass.interval_secs(),
        ));
    }
}

/// The one-shot startup peer snapshot.
pub struct StartupSnapshotActivation {
    handle: LoopHandle,
}

impl StartupSnapshotActivation {
    pub fn new(handle: LoopHandle) -> Self {
        Self { handle }
    }

    pub fn activate(self) {
        tokio::spawn(one_shot_cadence(
            self.handle,
            "startup-snapshot",
            STARTUP_SNAPSHOT_DELAY_SECONDS,
        ));
    }
}

/// The daily financial snapshot. Its interval is a constant in py, so
/// unlike the flow loop there is nothing to re-read between cycles.
pub struct FinancialCadenceActivation {
    handle: LoopHandle,
}

impl FinancialCadenceActivation {
    pub fn new(handle: LoopHandle) -> Self {
        Self { handle }
    }

    pub fn activate(self) {
        tokio::spawn(periodic_cadence(
            self.handle,
            "financial-snapshot",
            FINANCIAL_STARTUP_DELAY_SECONDS,
            FINANCIAL_JITTER_FRACTION,
            SchedulingRng::from_process_entropy(),
            || FINANCIAL_INTERVAL_SECONDS as u64,
        ));
    }
}
