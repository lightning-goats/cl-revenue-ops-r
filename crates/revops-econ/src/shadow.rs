//! Shadow hub core (port of `modules/econ_shadow.py`).
//!
//! `EconShadow` is the plugin's fail-open, observe-mode recording hub: it
//! journals real decisions into the append-only econ ledger and serves a
//! few small pieces of shared state (a TTL-cached canonical-snapshot
//! reference, a shared live-arbitration registry, an hourly reconciliation
//! throttle) WITHOUT holding any authority over execution. Every public
//! method here is FAIL-OPEN: none returns `Err`/panics into a caller — an
//! internal failure disables just the affected operation, logs once, and
//! returns `None`/a no-op/zero. This is the deliberate asymmetry against
//! `crate::governor`, which fails CLOSED — don't unify the two.
//!
//! ## Scope of this port (Task 10)
//!
//! The Python `EconShadow` class has more surface than is ported here.
//! This module ports exactly the six items named in the Task 10 "Mirrors"
//! line of `docs/superpowers/plans/2026-07-16-phase2-econ-core.md`, all of
//! which depend only on **config + clock + ledger** (the task's stated
//! injection surface) and carry no live plugin-produced data:
//!
//! 1. flag parsing (`'true'/'1'/'yes'/'on'` string-tolerant, vs. the
//!    strict-`is True` gates) — [`flag_tolerant_true`] / [`flag_strict_true`].
//! 2. [`default_ledger_path`] (port of `_default_ledger_path`).
//! 3. [`EconShadow::snapshot_ref`] — the TTL-cached canonical-snapshot
//!    reference (300s default, ledgered `snapshot_created`).
//! 4. the private `_journal` helper plus `note_spend_reserved/settled/
//!    released` (settle emits `cost_recorded` + `execution_succeeded` +
//!    `reservation_released`, IN ORDER; the reservation id doubles as both
//!    `intent_id` and `idempotency_key`).
//! 5. [`EconShadow::arbitration_registry`] — the shared
//!    `ActiveIntentRegistry`, gated by `econ_arbiter_enabled` with a live
//!    `econ_conflict_rules_extended` provider.
//! 6. [`EconShadow::maybe_run_reconciliation`] — the hourly self-throttled
//!    reconciliation sweep (plus [`EconShadow::ledger_for_reconciliation`],
//!    the raw-ledger accessor the reconciliation test harness and this
//!    sweep both need).
//!
//! Deliberately NOT ported here (all three take live plugin-produced data
//! — real fee-controller adjustments, real channel/profitability/budget
//! caches, real rebalance-engine candidate pairs — which is exactly the
//! "CLN plugin wiring" this task's brief defers to Phase 2b, alongside
//! `crate::cycle`'s `run_shadow_cycle` live collector):
//! `record_fee_intents`, `build_snapshot_preview`, and
//! `shadow_authorize_rebalance`.
//!
//! ## The float-in-explanation hazard (T8 review handoff)
//!
//! `crate::intents::Explanation::render()` deliberately PANICS on a
//! float-typed component (see that function's doc comment) — wire/
//! idempotency correctness demands it never silently emit a Rust
//! `f64::to_string()` that could diverge from Python's `repr(float)`.
//! The T8 review flagged that any "shadow journal path" building an
//! `Explanation` with a float component (e.g. a rebalance score) must
//! route around that panic, the way `crate::cycle`'s
//! `write_value_with_pyfloat` does for `CycleResult::canonical()`.
//!
//! This module was audited against that hazard: **none of the six ported
//! methods above ever constructs an `Explanation` or an `IntentEnvelope`**
//! — `_journal`/`note_spend_*` write only raw integer `amounts` +
//! plain-JSON `details` (matching `crate::ledger::EconLedger::append`'s
//! contract, which itself rejects float-typed JSON numbers fail-closed);
//! `snapshot_ref`'s `snapshot_created` event details carry only an integer
//! timestamp; `arbitration_registry`/`maybe_run_reconciliation` don't touch
//! explanations at all. So the panic-on-float guard is never exercised by
//! anything in this file, and no py_repr-aware rendering path was needed
//! here. The one Python method that DOES carry a float in an explanation
//! component — `shadow_authorize_rebalance`'s `rebalance_reservation`
//! explanation, whose `"score"` component is `float(pair.score)` — is out
//! of this task's scope (see above); whoever ports it in Phase 2b MUST
//! render its explanation string through a py_repr-aware path (mirroring
//! `crate::cycle::write_value_with_pyfloat`, NOT `Explanation::render()`)
//! rather than the panicking renderer, or it will panic the first time a
//! non-zero score reaches it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use serde_json::{json, Value};

use crate::arbiter::ActiveIntentRegistry;
use crate::ledger::EconLedger;
use crate::reconcile::{self, DbReservationState};
use crate::types::EconResult;

/// Mirrors the dynamically-typed values a CLN plugin option / JSON config
/// value can take for a boolean-ish setting: Python's
/// `getattr(config, name, default)` can yield a `bool`, a `str` (CLI/TOML
/// option), or be absent entirely. This crate has no dynamic attribute
/// access, so callers construct one of these explicitly from whatever
/// their real config representation holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigFlag {
    Bool(bool),
    Str(String),
    Missing,
}

impl From<bool> for ConfigFlag {
    fn from(v: bool) -> Self {
        ConfigFlag::Bool(v)
    }
}

impl From<&str> for ConfigFlag {
    fn from(v: &str) -> Self {
        ConfigFlag::Str(v.to_string())
    }
}

impl From<String> for ConfigFlag {
    fn from(v: String) -> Self {
        ConfigFlag::Str(v)
    }
}

/// Tolerant boolean coercion for `econ_shadow_enabled`, mirroring
/// `EconShadow.enabled()`'s two-branch dispatch: `isinstance(raw, str)` ->
/// trimmed/lowercased membership in `("true", "1", "yes", "on")`;
/// otherwise `raw is True` (so only an exact `bool` `true` counts — a
/// missing value, `Bool(false)`, or any other representation is `false`).
pub fn flag_tolerant_true(flag: &ConfigFlag) -> bool {
    match flag {
        ConfigFlag::Bool(b) => *b,
        ConfigFlag::Str(s) => matches!(
            s.trim().to_lowercase().as_str(),
            "true" | "1" | "yes" | "on"
        ),
        ConfigFlag::Missing => false,
    }
}

/// Strict boolean-identity gate for `econ_arbiter_enabled` and
/// `econ_conflict_rules_extended`, mirroring Python's `is not True` /
/// `is True` checks: ONLY an exact `bool` `true` counts. Deliberately
/// DIFFERENT from [`flag_tolerant_true`] — a string `"true"` does NOT
/// enable live arbitration or the extended conflict rules, even though it
/// WOULD enable shadow recording. Do not unify these two helpers.
pub fn flag_strict_true(flag: &ConfigFlag) -> bool {
    matches!(flag, ConfigFlag::Bool(true))
}

/// The three config values this module reads, resolved fresh by the
/// caller's `config` getter on every call — mirrors Python's
/// `self._config.snapshot()` (or the raw config object), read live so a
/// runtime flag flip needs no restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowConfigSnapshot {
    pub econ_shadow_enabled: ConfigFlag,
    pub econ_arbiter_enabled: ConfigFlag,
    pub econ_conflict_rules_extended: ConfigFlag,
}

/// Port of `EconShadow._default_ledger_path`: the econ ledger lives beside
/// `db_path` as `econ_ledger.db`. `db_path` absent/empty falls back to
/// `"~/.lightning/revenue_ops.db"` (Python's `... or "~/.lightning/..."`
/// falsy-or); `home_dir` is the injected `$HOME` equivalent for `~`
/// expansion — injected rather than read from the environment directly, so
/// this stays a pure, deterministically-testable function (consistent with
/// this whole port's "inject everything, no ambient state" discipline);
/// real callers pass `std::env::var("HOME").ok()`.
///
/// Python's version wraps the whole computation in `try/except Exception:
/// return "econ_ledger.db"`; there is no analogous fallible step here (no
/// dynamic attribute access, no OS calls) so the equivalent fail-open
/// fallback is structural: a `db_path` with no parent directory component
/// (e.g. a bare filename) degrades to the plain relative path
/// `"econ_ledger.db"`, exactly matching `os.path.join(os.path.dirname(x),
/// "econ_ledger.db")` when `dirname(x) == ""`.
pub fn default_ledger_path(db_path: Option<&str>, home_dir: Option<&str>) -> PathBuf {
    let raw = match db_path {
        Some(s) if !s.is_empty() => s,
        _ => "~/.lightning/revenue_ops.db",
    };
    let expanded = expand_user(raw, home_dir);
    match Path::new(&expanded).parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir.join("econ_ledger.db"),
        _ => PathBuf::from("econ_ledger.db"),
    }
}

/// Minimal `os.path.expanduser`-equivalent: only the leading `"~/"` and
/// bare `"~"` forms are expanded (the only forms `_default_ledger_path`
/// ever needs); `~user` is intentionally not supported.
fn expand_user(path: &str, home_dir: Option<&str>) -> String {
    let Some(home) = home_dir else {
        return path.to_string();
    };
    if let Some(rest) = path.strip_prefix("~/") {
        format!("{home}/{rest}")
    } else if path == "~" {
        home.to_string()
    } else {
        path.to_string()
    }
}

/// A TTL-cached reference to a freshly built canonical snapshot — port of
/// the dict `{"snapshot_id", "observed_at"}` returned by
/// `EconShadow.snapshot_ref`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRef {
    pub snapshot_id: String,
    pub observed_at: i64,
}

/// Default `max_age_seconds` for [`EconShadow::snapshot_ref`], matching
/// Python's `snapshot_ref(self, now, max_age_seconds=300)`.
pub const DEFAULT_SNAPSHOT_MAX_AGE_SECONDS: i64 = 300;

/// One reconciliation sweep's outcome — port of the `summary` dict
/// `maybe_run_reconciliation` returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationSummary {
    pub checked: usize,
    pub divergences: usize,
    pub applied: usize,
    pub quarantined: usize,
    pub completeness_ok: Option<bool>,
}

/// The database collaborator `maybe_run_reconciliation` needs, injected
/// per-call (not stored on `EconShadow`) since it's only needed for the
/// duration of one sweep — mirrors the Python method's `database`
/// parameter. Both accessors are fallible (`EconResult`) — the Rust
/// analogue of a Python `database.get_...()` call raising — but the two
/// failure modes are handled differently, exactly mirroring the Python
/// method's outer-vs-inner `try/except` split:
///
/// - `spend_reservation_states` failing aborts the WHOLE sweep fail-open
///   (`maybe_run_reconciliation` returns `None`, and — critically — the
///   throttle is NOT consumed, so the next call may retry sooner).
/// - `recent_fee_changes` failing is swallowed locally: the sweep still
///   completes normally (divergences/applied/quarantined are still
///   computed and returned), just with `completeness_ok: None`.
///
/// `recent_fee_changes`'s rows mirror `database.get_recent_fee_changes
/// (limit=500)`; each row is a JSON object with at least a `"timestamp"`
/// integer field (see `crate::reconcile::fee_intent_completeness`).
pub struct ReconciliationInputs<'a> {
    pub spend_reservation_states: &'a dyn Fn() -> EconResult<BTreeMap<String, DbReservationState>>,
    pub recent_fee_changes: &'a dyn Fn(usize) -> EconResult<Vec<Value>>,
}

/// `reconcile()`'s `stale_after_seconds` default (Python:
/// `def reconcile(..., stale_after_seconds: int = 3600)`).
const RECONCILE_STALE_AFTER_SECONDS: i64 = 3600;
/// `fee_intent_completeness()`'s defaults (Python: `window_seconds:
/// int = 86400, tolerance_seconds: int = 120`).
const FEE_COMPLETENESS_WINDOW_SECONDS: i64 = 86_400;
const FEE_COMPLETENESS_TOLERANCE_SECONDS: i64 = 120;
/// `get_recent_fee_changes`'s row-count cap (Python: `limit=500`).
const FEE_CHANGES_LIMIT: usize = 500;

/// Lock a `Mutex`, recovering the inner value on poison rather than
/// panicking — fail-open discipline extends to the module's own interior
/// mutability: a prior panic elsewhere must never turn every subsequent
/// call into an unwind here too.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The injected config getter: returns a fresh [`ShadowConfigSnapshot`] on
/// every call (mirrors `self._config.snapshot()`).
pub type ConfigGetter = Arc<dyn Fn() -> ShadowConfigSnapshot + Send + Sync>;
/// The injected clock: unix seconds "now" (mirrors Python's
/// `time.time()`), read only by the spend-journal path — see
/// [`EconShadow`]'s doc comment.
pub type ClockFn = Arc<dyn Fn() -> i64 + Send + Sync>;
/// The injected log sink: `(message, level)` (mirrors `plugin.log(message,
/// level=level)`).
pub type LogFn = Arc<dyn Fn(&str, &str) + Send + Sync>;
/// The canonical-snapshot provider: `() -> Option<(wire, approximations)>`
/// (mirrors the plugin-wired `shadow.snapshot_provider` callable).
pub type SnapshotProviderFn = Arc<dyn Fn() -> Option<(Value, Vec<String>)> + Send + Sync>;

/// The shadow hub itself. Every external dependency is injected: a
/// `config` getter (returns a fresh [`ShadowConfigSnapshot`] on every
/// call, so a runtime flag flip needs no restart), a `clock` (used only by
/// the spend-journal path, which — like the Python original's
/// `import time; time.time()` — reads wall-clock time itself rather than
/// accepting a `now` parameter), and a `log` sink (`(message, level)`,
/// matching `plugin.log(message, level=level)`). No CLN plugin wiring
/// lives here — see the module doc comment.
pub struct EconShadow {
    config: ConfigGetter,
    clock: ClockFn,
    log: LogFn,
    ledger_path: PathBuf,
    /// `None` once initialized = ledger permanently unavailable this
    /// session (mirrors `_ledger_failed`); the `OnceLock` guarantees the
    /// "unavailable" warn logs exactly once, no matter how many callers
    /// hit a broken ledger path.
    ledger_cell: OnceLock<Option<EconLedger>>,
    snapshot_provider: Mutex<Option<SnapshotProviderFn>>,
    snapshot_ref_cache: Mutex<Option<(SnapshotRef, i64)>>,
    /// Lazily built, then shared for the shadow's whole lifetime — "one
    /// registry per shadow instance, all governed paths consult the same
    /// state" (mirrors the Python class attribute `_intent_registry`).
    intent_registry: OnceLock<ActiveIntentRegistry>,
    last_reconcile_at: AtomicI64,
}

impl EconShadow {
    pub fn new(
        config: ConfigGetter,
        clock: ClockFn,
        log: LogFn,
        ledger_path: impl Into<PathBuf>,
    ) -> Self {
        EconShadow {
            config,
            clock,
            log,
            ledger_path: ledger_path.into(),
            ledger_cell: OnceLock::new(),
            snapshot_provider: Mutex::new(None),
            snapshot_ref_cache: Mutex::new(None),
            intent_registry: OnceLock::new(),
            last_reconcile_at: AtomicI64::new(0),
        }
    }

    /// Wires the canonical-snapshot provider (`() -> Option<(wire,
    /// approximations)>`), mirroring the Python plugin setting
    /// `shadow.snapshot_provider = fn` post-construction. Never called ==
    /// "no provider wired yet", matching the `__init__` default of `None`.
    pub fn set_snapshot_provider(
        &self,
        provider: impl Fn() -> Option<(Value, Vec<String>)> + Send + Sync + 'static,
    ) {
        *lock(&self.snapshot_provider) = Some(Arc::new(provider));
    }

    fn log_msg(&self, message: &str, level: &str) {
        (self.log)(&format!("ECON-SHADOW: {message}"), level);
    }

    /// Port of `EconShadow.enabled()`.
    pub fn enabled(&self) -> bool {
        flag_tolerant_true(&(self.config)().econ_shadow_enabled)
    }

    /// Lazily opens (and DDL-initializes) the ledger at most once for this
    /// instance's whole lifetime; every subsequent call — success or
    /// failure — returns the cached outcome without touching the
    /// filesystem again. Logs the "unavailable" warning exactly once, on
    /// the one call that actually attempts the open.
    fn get_ledger(&self) -> Option<&EconLedger> {
        self.ledger_cell
            .get_or_init(|| match EconLedger::open(&self.ledger_path) {
                Ok(ledger) => Some(ledger),
                Err(e) => {
                    self.log_msg(
                        &format!(
                            "ledger unavailable ({e}) — shadow recording disabled for this session"
                        ),
                        "warn",
                    );
                    None
                }
            })
            .as_ref()
    }

    /// The lazy ledger, for the reconciliation sweep (Phase 2 pilot B) and
    /// for tests that need to seed/inspect raw ledger state directly.
    /// `None` when disabled or unavailable.
    pub fn ledger_for_reconciliation(&self) -> Option<&EconLedger> {
        if !self.enabled() {
            return None;
        }
        self.get_ledger()
    }

    // ------------------------------------------------------------------
    // canonical snapshot service (PR 3a)
    // ------------------------------------------------------------------

    /// `snapshot_ref` with the Python default `max_age_seconds=300`.
    pub fn snapshot_ref(&self, now: i64) -> Option<SnapshotRef> {
        self.snapshot_ref_with_max_age(now, DEFAULT_SNAPSHOT_MAX_AGE_SECONDS)
    }

    /// TTL-cached reference to a freshly built canonical snapshot
    /// (fail-open — callers keep their pre-adoption synthetic labels on
    /// `None`). Each fresh build is ledgered as `snapshot_created` so
    /// intent `snapshot_id`s resolve against the ledger. Cache boundary is
    /// a strict `<`: `now - cached_at < max_age_seconds` is a hit,
    /// `now - cached_at >= max_age_seconds` rebuilds (so with the default
    /// 300s TTL, a 299s-old entry hits and a 300s-old entry rebuilds).
    pub fn snapshot_ref_with_max_age(&self, now: i64, max_age_seconds: i64) -> Option<SnapshotRef> {
        if !self.enabled() {
            return None;
        }
        let provider = lock(&self.snapshot_provider).clone()?;

        if let Some((cached_ref, cached_at)) = lock(&self.snapshot_ref_cache).clone() {
            if now - cached_at < max_age_seconds {
                return Some(cached_ref);
            }
        }

        let (wire, _approximations) = provider()?;
        let snapshot_id = wire
            .get("snapshot_id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())?
            .to_string();
        let observed_at = wire
            .get("observed_at")
            .and_then(Value::as_i64)
            .unwrap_or(now);
        let snap_ref = SnapshotRef {
            snapshot_id,
            observed_at,
        };
        *lock(&self.snapshot_ref_cache) = Some((snap_ref.clone(), now));

        if let Some(ledger) = self.get_ledger() {
            // Best-effort: a failure to ledger the snapshot build must
            // never invalidate the snapshot reference itself.
            let _ = ledger.append(
                "snapshot_created",
                &snap_ref.snapshot_id,
                &snap_ref.snapshot_id,
                &snap_ref.snapshot_id,
                now,
                &BTreeMap::new(),
                &json!({"observed_at": snap_ref.observed_at}),
            );
        }
        Some(snap_ref)
    }

    // ------------------------------------------------------------------
    // legacy spend-path journal (Phase 2 pilot A)
    // ------------------------------------------------------------------

    /// Port of the private `_journal` helper. `category` empty maps to
    /// `cycle_id = "spend-generic"`, matching Python's `str(category or
    /// 'generic')`. Uses the injected `clock`, not a caller-supplied `now`
    /// — matches Python's `_journal` reading `time.time()` directly rather
    /// than accepting a timestamp argument (unlike every other method in
    /// this class).
    fn journal(
        &self,
        event_type: &str,
        reservation_id: &str,
        category: &str,
        amounts: BTreeMap<String, i64>,
        details: Value,
    ) {
        if !self.enabled() {
            return;
        }
        let rid = reservation_id.trim();
        if rid.is_empty() {
            return;
        }
        let Some(ledger) = self.get_ledger() else {
            return;
        };
        let cycle_id = format!(
            "spend-{}",
            if category.is_empty() {
                "generic"
            } else {
                category
            }
        );
        let now = (self.clock)();
        if let Err(e) = ledger.append(event_type, rid, rid, &cycle_id, now, &amounts, &details) {
            self.log_msg(
                &format!("spend journal skipped ({event_type}): {e}"),
                "debug",
            );
        }
    }

    /// Port of `note_spend_reserved`. `amount_sats * 1000` overflowing i64
    /// is this Rust port's analogue of Python's caught `TypeError`/
    /// `ValueError` on `int(amount_sats)` — both are "the caller handed us
    /// something we can't turn into a valid msat amount", fail-open the
    /// same way: skip, log at debug, no-op.
    pub fn note_spend_reserved(&self, reservation_id: &str, amount_sats: i64, category: &str) {
        let Some(reserved_msat) = amount_sats.checked_mul(1000) else {
            self.log_msg(
                &format!("note_spend_reserved skipped: bad amount {amount_sats}"),
                "debug",
            );
            return;
        };
        let mut amounts = BTreeMap::new();
        amounts.insert("reserved_msat".to_string(), reserved_msat);
        self.journal(
            "budget_reserved",
            reservation_id,
            category,
            amounts,
            json!({}),
        );
    }

    /// Port of `note_spend_settled`. Emits, IN ORDER: `cost_recorded` ->
    /// `execution_succeeded` -> `reservation_released` (`{"reason":
    /// "settled"}`) — settle is terminal for the whole reservation, so the
    /// unused remainder is released alongside it (spec reservation
    /// machine: `reserved -> spent`, unused portion -> `released`).
    pub fn note_spend_settled(&self, reservation_id: &str, actual_spent_sats: i64, category: &str) {
        let Some(cost_msat) = actual_spent_sats.checked_mul(1000) else {
            self.log_msg(
                &format!("note_spend_settled skipped: bad amount {actual_spent_sats}"),
                "debug",
            );
            return;
        };
        let mut amounts = BTreeMap::new();
        amounts.insert("cost_msat".to_string(), cost_msat);
        self.journal(
            "cost_recorded",
            reservation_id,
            category,
            amounts,
            json!({}),
        );
        self.journal(
            "execution_succeeded",
            reservation_id,
            category,
            BTreeMap::new(),
            json!({}),
        );
        self.journal(
            "reservation_released",
            reservation_id,
            category,
            BTreeMap::new(),
            json!({"reason": "settled"}),
        );
    }

    /// Port of `note_spend_released` (Python default `reason="released"` —
    /// pass `"released"` explicitly; Rust has no default arguments).
    pub fn note_spend_released(&self, reservation_id: &str, reason: &str) {
        self.journal(
            "reservation_released",
            reservation_id,
            "",
            BTreeMap::new(),
            json!({"reason": reason}),
        );
    }

    // ------------------------------------------------------------------
    // live arbitration (Phase 3F)
    // ------------------------------------------------------------------

    /// The shared [`ActiveIntentRegistry`] when live arbitration is
    /// enabled (`econ_arbiter_enabled`, STRICT `is True` gate — see
    /// [`flag_strict_true`]), else `None`. Built once and reused for the
    /// whole life of this `EconShadow` — every governed path that calls
    /// this consults the same live state. The registry's
    /// `extended_rules_provider` re-reads `econ_conflict_rules_extended`
    /// (also strict) through the same `config` getter on every check, so a
    /// live flag flip takes effect without rebuilding the registry.
    pub fn arbitration_registry(&self) -> Option<&ActiveIntentRegistry> {
        if !flag_strict_true(&(self.config)().econ_arbiter_enabled) {
            return None;
        }
        Some(self.intent_registry.get_or_init(|| {
            let config = Arc::clone(&self.config);
            let extended_rules_provider: Box<dyn Fn() -> bool + Send + Sync> =
                Box::new(move || flag_strict_true(&config().econ_conflict_rules_extended));
            ActiveIntentRegistry::new(Some(extended_rules_provider))
        }))
    }

    // ------------------------------------------------------------------
    // automated reconciliation sweep (Phase 2I)
    // ------------------------------------------------------------------

    /// Hourly (by default) self-throttled sweep: auto-applies resolvable
    /// ledger divergences (append-only corrections toward DB truth),
    /// WARNS on quarantined unknown outcomes every sweep until reconciled,
    /// and WARNS on fee-intent completeness gaps. Writes only the econ
    /// ledger; never the caller's database. Fail-open: `None` on any
    /// failure, disablement, missing `inputs`, or throttle.
    ///
    /// Throttle semantics: `now - last_successful_at < min_interval_seconds`
    /// skips (returns `None` WITHOUT attempting a sweep); the throttle
    /// clock only advances on a fully successful sweep (see
    /// [`ReconciliationInputs`]'s doc comment for the outer-vs-inner
    /// failure split this mirrors from Python).
    pub fn maybe_run_reconciliation(
        &self,
        inputs: Option<&ReconciliationInputs>,
        now: i64,
        min_interval_seconds: i64,
    ) -> Option<ReconciliationSummary> {
        if !self.enabled() {
            return None;
        }
        let inputs = inputs?;
        let last = self.last_reconcile_at.load(Ordering::SeqCst);
        if now - last < min_interval_seconds {
            return None;
        }
        let ledger = self.get_ledger()?;
        match self.run_reconciliation_sweep(ledger, inputs, now) {
            Ok(summary) => Some(summary),
            Err(e) => {
                self.log_msg(&format!("reconciliation sweep failed open: {e}"), "debug");
                None
            }
        }
    }

    fn run_reconciliation_sweep(
        &self,
        ledger: &EconLedger,
        inputs: &ReconciliationInputs,
        now: i64,
    ) -> EconResult<ReconciliationSummary> {
        // Outer failure surface: a broken DB read here aborts the whole
        // sweep fail-open (propagated to the caller via `?`, caught by
        // `maybe_run_reconciliation`'s match arm) WITHOUT consuming the
        // throttle — mirrors Python's outer `try/except` wrapping this
        // exact sequence.
        let db_states = (inputs.spend_reservation_states)()?;
        let report = reconcile::reconcile(ledger, &db_states, now, RECONCILE_STALE_AFTER_SECONDS)?;
        let applied = reconcile::apply(ledger, &report, now)?;
        let quarantined_count = report
            .divergences
            .iter()
            .filter(|d| d.resolution.is_none())
            .count();
        for divergence in report.divergences.iter().filter(|d| d.resolution.is_none()) {
            let age = divergence
                .details
                .get("age_seconds")
                .cloned()
                .unwrap_or(Value::Null);
            let key_prefix: String = divergence.key.chars().take(16).collect();
            self.log_msg(
                &format!(
                    "quarantined unknown outcome awaiting reconciliation \
                     (EXTERNAL_OUTCOME_UNKNOWN): {key_prefix} age={age}s"
                ),
                "warn",
            );
        }

        // Inner failure surface: swallowed locally, mirroring Python's
        // inner `try/except` around the fee-completeness cross-check —
        // its failure (either the fee-changes read or the completeness
        // computation itself) never aborts the sweep, it only leaves
        // `completeness_ok` at `None`.
        let mut completeness_ok: Option<bool> = None;
        let completeness_result =
            (inputs.recent_fee_changes)(FEE_CHANGES_LIMIT).and_then(|fee_changes| {
                reconcile::fee_intent_completeness(
                    ledger,
                    &fee_changes,
                    now,
                    FEE_COMPLETENESS_WINDOW_SECONDS,
                    FEE_COMPLETENESS_TOLERANCE_SECONDS,
                )
            });
        match completeness_result {
            Ok(completeness) => {
                if completeness.get("status").and_then(Value::as_str) == Some("ok") {
                    let ok = completeness
                        .get("complete")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    completeness_ok = Some(ok);
                    if !ok {
                        let mismatched = completeness
                            .get("mismatched_cycles")
                            .cloned()
                            .unwrap_or(Value::Null);
                        self.log_msg(
                            &format!("fee-intent completeness gap: {mismatched}"),
                            "warn",
                        );
                    }
                }
            }
            Err(e) => self.log_msg(&format!("completeness check skipped: {e}"), "debug"),
        }

        self.last_reconcile_at.store(now, Ordering::SeqCst);
        let summary = ReconciliationSummary {
            checked: report.checked,
            divergences: report.divergences.len(),
            applied,
            quarantined: quarantined_count,
            completeness_ok,
        };
        let level = if !report.divergences.is_empty() || applied > 0 {
            "info"
        } else {
            "debug"
        };
        self.log_msg(&format!("reconciliation sweep: {summary:?}"), level);
        Ok(summary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- flag tolerance table ---

    #[test]
    fn flag_tolerant_true_accepts_exact_bool_true_only_for_non_strings() {
        assert!(flag_tolerant_true(&ConfigFlag::Bool(true)));
        assert!(!flag_tolerant_true(&ConfigFlag::Bool(false)));
        assert!(!flag_tolerant_true(&ConfigFlag::Missing));
    }

    #[test]
    fn flag_tolerant_true_string_table() {
        let truthy = [
            "true", "TRUE", " True ", "1", "yes", "YES", "on", "ON", " on\n",
        ];
        for s in truthy {
            assert!(
                flag_tolerant_true(&ConfigFlag::from(s)),
                "expected truthy: {s:?}"
            );
        }
        let falsy = [
            "false",
            "FALSE",
            "0",
            "no",
            "off",
            "",
            "garbage",
            "2",
            "yesplease",
        ];
        for s in falsy {
            assert!(
                !flag_tolerant_true(&ConfigFlag::from(s)),
                "expected falsy: {s:?}"
            );
        }
    }

    #[test]
    fn flag_strict_true_rejects_string_and_only_accepts_bool_true() {
        assert!(flag_strict_true(&ConfigFlag::Bool(true)));
        assert!(!flag_strict_true(&ConfigFlag::Bool(false)));
        assert!(!flag_strict_true(&ConfigFlag::Missing));
        // The deliberate divergence from flag_tolerant_true: a tolerant
        // truthy string must NOT satisfy the strict gate.
        assert!(!flag_strict_true(&ConfigFlag::from("true")));
        assert!(!flag_strict_true(&ConfigFlag::from("1")));
    }

    // --- default_ledger_path ---

    #[test]
    fn default_ledger_path_joins_dirname_of_db_path() {
        let p = default_ledger_path(Some("/home/user/.lightning/revenue_ops.db"), None);
        assert_eq!(p, PathBuf::from("/home/user/.lightning/econ_ledger.db"));
    }

    #[test]
    fn default_ledger_path_falls_back_when_db_path_absent_or_empty() {
        let via_none = default_ledger_path(None, Some("/home/user"));
        let via_empty = default_ledger_path(Some(""), Some("/home/user"));
        let expected = PathBuf::from("/home/user/.lightning/econ_ledger.db");
        assert_eq!(via_none, expected);
        assert_eq!(via_empty, expected);
    }

    #[test]
    fn default_ledger_path_expands_leading_tilde() {
        let p = default_ledger_path(Some("~/data/revenue_ops.db"), Some("/home/hex"));
        assert_eq!(p, PathBuf::from("/home/hex/data/econ_ledger.db"));
    }

    #[test]
    fn default_ledger_path_without_home_leaves_tilde_literal() {
        // No injected home -> nothing to expand against; degrades to the
        // literal path's dirname rather than panicking or guessing.
        let p = default_ledger_path(Some("~/data/revenue_ops.db"), None);
        assert_eq!(p, PathBuf::from("~/data/econ_ledger.db"));
    }

    #[test]
    fn default_ledger_path_bare_filename_has_no_parent() {
        // os.path.dirname("revenue_ops.db") == "" -> os.path.join("",
        // "econ_ledger.db") == "econ_ledger.db".
        let p = default_ledger_path(Some("revenue_ops.db"), None);
        assert_eq!(p, PathBuf::from("econ_ledger.db"));
    }
}
